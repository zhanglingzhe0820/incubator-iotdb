/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.merge.sizeMerge.simple.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.IMergePathSelector;
import org.apache.iotdb.db.engine.merge.NaivePathSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.simple.recover.SimpleMergeLogger;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.MergeUtils.MetaListEntry;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.iotdb.db.engine.merge.seqmerge.squeeze.task.SqueezeMergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

class MergeSeriesTask {

    private static final Logger logger = LoggerFactory.getLogger(
            MergeSeriesTask.class);
    private static int minChunkPointNum = IoTDBDescriptor.getInstance().getConfig()
            .getChunkMergePointThreshold();

    private SimpleMergeLogger mergeLogger;
    private List<Path> unmergedSeries;

    private String taskName;
    private MergeResource resource;
    private TimeValuePair[] currTimeValuePairs;

    private MergeContext mergeContext;

    private int mergedSeriesCnt;
    private double progress;

    private int concurrentMergeSeriesNum;
    private List<Path> currMergingPaths = new ArrayList<>();

    private RestorableTsFileIOWriter newFileWriter;
    private TsFileResource newResource;
    private String currDevice = null;

    MergeSeriesTask(MergeContext context, String taskName, SimpleMergeLogger mergeLogger,
                    MergeResource mergeResource, List<Path> unmergedSeries,
                    int concurrentMergeSeriesNum) {
        this.mergeContext = context;
        this.taskName = taskName;
        this.mergeLogger = mergeLogger;
        this.resource = mergeResource;
        this.unmergedSeries = unmergedSeries;
        this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    }

    TsFileResource mergeSeries() throws IOException {
        if (logger.isInfoEnabled()) {
            logger.debug("{} starts to merge {} series", taskName, resource.getSeqFiles().size());
        }
        long startTime = System.currentTimeMillis();

        createNewFileWriter();
        mergePaths(resource.getSeqFiles());

        newFileWriter.endFile(new Schema(newFileWriter.getKnownSchema()));
        // the new file is ready to replace the old ones, write logs so we will not need to start from
        // the beginning after system failure
        mergeLogger.logAllTsEnd();
        mergeLogger.logNewFile(newResource);

        if (logger.isInfoEnabled()) {
            logger.debug("{} all series are merged after {}ms", taskName,
                    System.currentTimeMillis() - startTime);
        }

        return newResource;
    }

    private void mergePaths(List<TsFileResource> tsFileResourceList) throws IOException {
        IPointReader[] unseqReaders;
        unseqReaders = resource.getUnseqReaders(currMergingPaths);
        currTimeValuePairs = new TimeValuePair[currMergingPaths.size()];
        for (int i = 0; i < currMergingPaths.size(); i++) {
            if (unseqReaders[i].hasNext()) {
                currTimeValuePairs[i] = unseqReaders[i].current();
            }
        }

        mergeChunks(tsFileResourceList);

        newFileWriter.endChunkGroup(0);
        currDevice = null;
    }

    private void createNewFileWriter() throws IOException {
        // use the minimum version as the version of the new file
        long currFileVersion =
                Long.parseLong(
                        resource.getSeqFiles().get(0).getFile().getName().replace(TSFILE_SUFFIX, "").split(TSFILE_SEPARATOR)[1]);
        long prevMergeNum =
                Long.parseLong(
                        resource.getSeqFiles().get(0).getFile().getName().replace(TSFILE_SUFFIX, "").split(TSFILE_SEPARATOR)[2]);
        File parent = resource.getSeqFiles().get(0).getFile().getParentFile();
        File newFile = FSFactoryProducer.getFSFactory().getFile(parent,
                System.currentTimeMillis() + TSFILE_SEPARATOR + currFileVersion + TSFILE_SEPARATOR + (prevMergeNum + 1) + TSFILE_SUFFIX + MERGE_SUFFIX);
        newFileWriter = new RestorableTsFileIOWriter(newFile);
        newResource = new TsFileResource(newFile);
    }

    private boolean allPathEmpty(List[] seqChunkMeta) {
        for (int i = 0; i < seqChunkMeta.length; i++) {
            if (!seqChunkMeta[i].isEmpty() || currTimeValuePairs[i] != null) {
                return false;
            }
        }
        return true;
    }

    private long mergeChunks(List<TsFileResource> tsFileResourceList)
            throws IOException {
        int mergeChunkSubTaskNum = IoTDBDescriptor.getInstance().getConfig().getMergeChunkSubThreadNum();
        List<TsFileResource>[] chunkMetaLists = (List<TsFileResource>[]) new ArrayList[mergeChunkSubTaskNum];
        for (int i = 0; i < mergeChunkSubTaskNum; i++) {
            chunkMetaLists[i] = new ArrayList<>();
        }
        int averageNum = tsFileResourceList.size() / mergeChunkSubTaskNum;
        int idx;
        for (int i = 0; i < mergeChunkSubTaskNum - 1; i++) {
            idx = averageNum * i;
            List<TsFileResource> subTsFileResources = new ArrayList<>();
            for (int j = 0; j < averageNum; j++) {
                subTsFileResources.add(tsFileResourceList.get(idx + j));
            }
            chunkMetaLists[i] = subTsFileResources;
        }
        idx = averageNum * (mergeChunkSubTaskNum - 1);
        if (idx >= 0) {
            List<TsFileResource> subTsFileResources = new ArrayList<>();
            for (int j = idx; j < tsFileResourceList.size(); j++) {
                subTsFileResources.add(tsFileResourceList.get(idx + j));
            }
            chunkMetaLists[mergeChunkSubTaskNum - 1] = subTsFileResources;
        }

        List<Future<Long>> futures = new ArrayList<>();
        for (int i = 0; i < mergeChunkSubTaskNum; i++) {
            int finalI = i;
            futures.add(MergeManager.getINSTANCE().submitChunkSubTask(() -> mergeChunkHeap(chunkMetaLists[finalI])));
        }
        long maxTime = Long.MIN_VALUE;
        for (int i = 0; i < mergeChunkSubTaskNum; i++) {
            try {
                Long heapMaxTimeStamp = futures.get(i).get();
                maxTime = Math.max(maxTime, heapMaxTimeStamp);
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
        }
        return maxTime;
    }

    private long mergeChunkHeap(List<TsFileResource> tsFileResources)
            throws IOException {
        long maxTime = Long.MIN_VALUE;
        while (!chunkMetaHeap.isEmpty()) {
            MetaListEntry metaListEntry = chunkMetaHeap.poll();
            ChunkMetaData currMeta = metaListEntry.current();
            int pathIdx = metaListEntry.getPathId();
            boolean isLastChunk = !metaListEntry.hasNext();
            Path path = currMergingPaths.get(pathIdx);
            MeasurementSchema measurementSchema = resource.getSchema(path.getMeasurement());
            IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);

            boolean chunkOverflowed = MergeUtils.isChunkOverflowed(currTimeValuePairs[pathIdx], currMeta);
            boolean chunkTooSmall =
                    chunkWriter.getPtNum() > 0 || currMeta.getNumOfPoints() < minChunkPointNum;

            Chunk chunk;
            synchronized (reader) {
                chunk = reader.readMemChunk(currMeta);
            }

            long maxMergedTime = mergeChunkV2(currMeta, chunkOverflowed, chunkTooSmall, chunk, pathIdx,
                    unseqReaders[pathIdx], chunkWriter);
            maxTime = Math.max(maxMergedTime, maxTime);

            if (!isLastChunk) {
                metaListEntry.next();
                chunkMetaHeap.add(metaListEntry);
            }
        }
        return maxTime;
    }

    private long mergeChunkV2(ChunkMetaData currMeta, boolean chunkOverflowed,
                              boolean chunkTooSmall, Chunk chunk, int pathIdx,
                              IPointReader unseqReader, IChunkWriter chunkWriter) throws IOException {

        // write the chunk to .merge.file without compressing
        if (chunkWriter.getPtNum() == 0 && !chunkTooSmall && !chunkOverflowed) {
            synchronized (newFileWriter) {
                newFileWriter.writeChunk(chunk, currMeta);
            }
            return currMeta.getEndTime();
        }

        // uncompress and write the chunk
        writeChunkWithUnseq(chunk, chunkWriter, unseqReader, pathIdx);

        // check chunk size for flush and update points written statistics
        if (minChunkPointNum > 0 && chunkWriter.getPtNum() >= minChunkPointNum
                || chunkWriter.getPtNum() > 0 && minChunkPointNum < 0) {
            // the new chunk's size is large enough and it should be flushed
            synchronized (newFileWriter) {
                mergeContext.incTotalPointWritten(chunkWriter.getPtNum());
                chunkWriter.writeToFileWriter(newFileWriter);
            }
        }
        return currMeta.getEndTime();
    }

    private void writeChunkWithUnseq(Chunk chunk, IChunkWriter chunkWriter, IPointReader unseqReader,
                                     int pathIdx) throws IOException {
        ChunkReader chunkReader = new ChunkReader(chunk, null);
        while (chunkReader.hasNextSatisfiedPage()) {
            BatchData batchData = chunkReader.nextPageData();
            mergeWriteBatch(batchData, chunkWriter, unseqReader, pathIdx);
        }
    }

    private void mergeWriteBatch(BatchData batchData, IChunkWriter chunkWriter,
                                 IPointReader unseqReader, int pathIdx) throws IOException {
        for (int i = 0; i < batchData.length(); i++) {
            long time = batchData.getTimeByIndex(i);
            // merge data in batch and data in unseqReader

            boolean overwriteSeqPoint = false;
            // unseq point.time <= sequence point.time, write unseq point
            while (currTimeValuePairs[pathIdx] != null
                    && currTimeValuePairs[pathIdx].getTimestamp() <= time) {
                writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
                if (currTimeValuePairs[pathIdx].getTimestamp() == time) {
                    overwriteSeqPoint = true;
                }
                unseqReader.next();
                currTimeValuePairs[pathIdx] = unseqReader.hasNext() ? unseqReader.current() : null;
            }
            // unseq point.time > sequence point.time, write seq point
            if (!overwriteSeqPoint) {
                writeBatchPoint(batchData, i, chunkWriter);
            }
        }
    }
}
