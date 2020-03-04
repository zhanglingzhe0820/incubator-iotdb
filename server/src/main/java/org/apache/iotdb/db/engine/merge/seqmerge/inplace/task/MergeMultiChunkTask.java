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

package org.apache.iotdb.db.engine.merge.seqmerge.inplace.task;

import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.seqmerge.inplace.recover.InplaceMergeLogger;
import org.apache.iotdb.db.engine.merge.IMergePathSelector;
import org.apache.iotdb.db.engine.merge.NaivePathSelector;
import org.apache.iotdb.tsfile.read.common.util.ChunkProvider;
import org.apache.iotdb.tsfile.read.common.util.SharedMapChunkProvider;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.MergeUtils.MetaListEntry;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MergeMultiChunkTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeMultiChunkTask.class);
  private static int minChunkPointNum = IoTDBDescriptor.getInstance().getConfig()
      .getChunkMergePointThreshold();

  private InplaceMergeLogger mergeLogger;
  private List<Path> unmergedSeries;

  private String taskName;
  private MergeResource resource;
  private TimeValuePair[] currTimeValuePairs;
  private boolean fullMerge;

  private MergeContext mergeContext;

  private AtomicInteger mergedChunkNum = new AtomicInteger();
  private AtomicInteger unmergedChunkNum = new AtomicInteger();
  private int mergedSeriesCnt;
  private double progress;

  private int concurrentMergeSeriesNum;
  private List<Path> currMergingPaths = new ArrayList<>();

  MergeMultiChunkTask(MergeContext context, String taskName, InplaceMergeLogger mergeLogger,
      MergeResource mergeResource, boolean fullMerge, List<Path> unmergedSeries,
      int concurrentMergeSeriesNum) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.fullMerge = fullMerge;
    this.unmergedSeries = unmergedSeries;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
  }

  void mergeSeries() throws IOException {
    if (logger.isInfoEnabled()) {
      logger.debug("{} starts to merge {} series", taskName, unmergedSeries.size());
    }
    long startTime = System.currentTimeMillis();
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      mergeContext.getUnmergedChunkStartTimes().put(seqFile, new HashMap<>());
    }
    // merge each series and write data into each seqFile's corresponding temp merge file
    List<List<Path>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<Path> pathList : devicePaths) {
      // TODO: use statistics of queries to better rearrange series
      IMergePathSelector pathSelector = new NaivePathSelector(pathList, concurrentMergeSeriesNum);
      while (pathSelector.hasNext()) {
        currMergingPaths = pathSelector.next();
        mergePaths();
        mergedSeriesCnt += currMergingPaths.size();
        logMergeProgress();
      }
    }
    if (logger.isInfoEnabled()) {
      logger.debug("{} all series are merged after {}ms", taskName,
          System.currentTimeMillis() - startTime);
    }
    mergeLogger.logAllTsEnd();
  }

  private void logMergeProgress() {
    if (logger.isInfoEnabled()) {
      double newProgress = 100 * mergedSeriesCnt / (double) (unmergedSeries.size());
      if (newProgress - progress >= 1.0) {
        progress = newProgress;
        logger.debug("{} has merged {}% series", taskName, progress);
      }
    }
  }

  private void mergePaths() throws IOException {
    mergeLogger.logTSStart(currMergingPaths);
    IPointReader[] unseqReaders;
    unseqReaders = resource.getUnseqReaders(currMergingPaths);
    currTimeValuePairs = new TimeValuePair[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (unseqReaders[i].hasNext()) {
        currTimeValuePairs[i] = unseqReaders[i].current();
      }
    }

    for (int i = 0; i < resource.getSeqFiles().size(); i++) {
      pathsMergeOneFile(i, unseqReaders);
    }
    mergeLogger.logTSEnd();
  }

  private void pathsMergeOneFile(int seqFileIdx, IPointReader[] unseqReaders)
      throws IOException {
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    String deviceId = currMergingPaths.get(0).getDevice();
    Long currDeviceMinTime = currTsFile.getStartTimeMap().get(deviceId);
    if (currDeviceMinTime == null) {
      return;
    }

    for (Path path : currMergingPaths) {
      mergeContext.getUnmergedChunkStartTimes().get(currTsFile).put(path, new ArrayList<>());
    }

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    for (TimeValuePair timeValuePair : currTimeValuePairs) {
      if (timeValuePair != null && timeValuePair.getTimestamp() < currDeviceMinTime) {
        currDeviceMinTime = timeValuePair.getTimestamp();
      }
    }
    boolean isLastFile = seqFileIdx + 1 == resource.getSeqFiles().size();

    TsFileSequenceReader fileSequenceReader = resource.getFileReader(currTsFile);
    List<Modification> modifications;
    List<ChunkMetaData>[] seqChunkMeta = new List[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      modifications = resource.getModifications(currTsFile, currMergingPaths.get(i));
      seqChunkMeta[i] = resource.queryChunkMetadata(currMergingPaths.get(i), currTsFile);
      modifyChunkMetaData(seqChunkMeta[i], modifications);
    }

    List<Integer> unskippedPathIndices = filterNoDataPaths(seqChunkMeta, seqFileIdx);
    if (unskippedPathIndices.isEmpty()) {
      return;
    }

    RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(currTsFile);
    for (Path path : currMergingPaths) {
      MeasurementSchema schema = resource.getSchema(path.getMeasurement());
      mergeFileWriter.addSchema(schema);
    }
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    mergeFileWriter.startChunkGroup(deviceId);
    boolean dataWritten = mergeChunks(seqChunkMeta, isLastFile, fileSequenceReader, unseqReaders,
        mergeFileWriter, currTsFile);
    if (dataWritten) {
      mergeFileWriter.endChunkGroup(0);
      mergeLogger.logFilePosition(mergeFileWriter.getFile());
      currTsFile.getStartTimeMap().put(deviceId, currDeviceMinTime);
    }
  }

  private List<Integer> filterNoDataPaths(List[] seqChunkMeta, int seqFileIdx) {
    // if the last seqFile does not contains this series but the unseqFiles do, data of this
    // series should also be written into a new chunk
    List<Integer> ret = new ArrayList<>();
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (seqChunkMeta[i].isEmpty()
          && !(seqFileIdx + 1 == resource.getSeqFiles().size() && currTimeValuePairs[i] != null)) {
        continue;
      }
      ret.add(i);
    }
    return ret;
  }

  private boolean mergeChunks(List<ChunkMetaData>[] seqChunkMeta, boolean isLastFile,
      TsFileSequenceReader reader, IPointReader[] unseqReaders,
      RestorableTsFileIOWriter mergeFileWriter, TsFileResource currFile)
      throws IOException {
    int[] ptWrittens = new int[seqChunkMeta.length];
    int mergeChunkSubTaskNum = IoTDBDescriptor.getInstance().getConfig().getMergeChunkSubThreadNum();
    PriorityQueue<MetaListEntry>[] chunkMetaHeaps = new PriorityQueue[mergeChunkSubTaskNum];
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      chunkMetaHeaps[i] = new PriorityQueue<>();
    }
    int idx = 0;
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (seqChunkMeta[i].isEmpty()) {
        continue;
      }
      MetaListEntry entry = new MetaListEntry(i, seqChunkMeta[i]);
      entry.next();

      chunkMetaHeaps[idx % mergeChunkSubTaskNum].add(entry);
      idx++;
      ptWrittens[i] = 0;
    }
//    ChunkProvider provider = new DirectChunkProvider(reader);
    ChunkProvider provider = new SharedMapChunkProvider(seqChunkMeta, reader);

    mergedChunkNum.set(0);
    unmergedChunkNum.set(0);

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      int finalI = i;
      futures.add(MergeManager.getINSTANCE().submitChunkSubTask(() -> {
        mergeChunkHeap(chunkMetaHeaps[finalI], ptWrittens, provider, mergeFileWriter, unseqReaders,
            currFile, isLastFile);
        return null;
      }));
    }
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      try {
        futures.get(i).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }

    // add merge and unmerged chunk statistic
    mergeContext.getMergedChunkCnt().compute(currFile, (tsFileResource, anInt) -> anInt == null ?
        mergedChunkNum.get() : anInt + mergedChunkNum.get());
    mergeContext.getUnmergedChunkCnt().compute(currFile, (tsFileResource, anInt) -> anInt == null ?
        unmergedChunkNum.get() : anInt + unmergedChunkNum.get());

    return mergedChunkNum.get() > 0;
  }

  private void mergeChunkHeap(PriorityQueue<MetaListEntry> chunkMetaHeap, int[] ptWrittens,
      ChunkProvider provider, RestorableTsFileIOWriter mergeFileWriter, IPointReader[] unseqReaders,
      TsFileResource currFile,
      boolean isLastFile) throws IOException {
    while (!chunkMetaHeap.isEmpty()) {
      MetaListEntry metaListEntry = chunkMetaHeap.poll();
      ChunkMetaData currMeta = metaListEntry.current();
      int pathIdx = metaListEntry.getPathId();
      boolean isLastChunk = !metaListEntry.hasNext();
      Path path = currMergingPaths.get(pathIdx);
      MeasurementSchema measurementSchema = resource.getSchema(path.getMeasurement());
      IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);

      boolean chunkOverflowed = MergeUtils.isChunkOverflowed(currTimeValuePairs[pathIdx], currMeta);
      boolean chunkTooSmall = MergeUtils
          .isChunkTooSmall(ptWrittens[pathIdx], currMeta, isLastChunk, minChunkPointNum);

      Chunk chunk;
      try {
        chunk = provider.require(currMeta);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted when reading a chunk", e);
      }

      ptWrittens[pathIdx] = mergeChunkV2(currMeta, chunkOverflowed, chunkTooSmall, chunk,
              ptWrittens[pathIdx], pathIdx, mergeFileWriter, unseqReaders[pathIdx], chunkWriter,
              currFile);

      if (!isLastChunk) {
        metaListEntry.next();
        chunkMetaHeap.add(metaListEntry);
      } else {
        // this only happens when the seqFiles do not contain this series, otherwise the remaining
        // data will be merged with the last chunk in the seqFiles
        if (isLastFile && currTimeValuePairs[pathIdx] != null) {
          ptWrittens[pathIdx] += writeRemainingUnseq(chunkWriter, unseqReaders[pathIdx], pathIdx);
          mergedChunkNum.incrementAndGet();
        }
        // the last merged chunk may still be smaller than the threshold, flush it anyway
        if (ptWrittens[pathIdx] > 0) {
          synchronized (mergeFileWriter) {
            chunkWriter.writeToFileWriter(mergeFileWriter);
          }
        }
      }
    }
  }

  /**
   * merge a sequence chunk SK
   *
   * 1. no need to write the chunk to .merge file when:
   * isn't full merge &
   * there isn't unclosed chunk before &
   * SK is big enough &
   * SK isn't overflowed &
   * SK isn't modified
   *
   *
   * 2. write SK to .merge.file without compressing when:
   * is full merge &
   * there isn't unclosed chunk before &
   * SK is big enough &
   * SK isn't overflowed &
   * SK isn't modified
   *
   * 3. other cases: need to unCompress the chunk and write
   * 3.1 SK isn't overflowed
   * 3.2 SK is overflowed
   *
   */
  private int mergeChunkV2(ChunkMetaData currMeta, boolean chunkOverflowed,
      boolean chunkTooSmall,Chunk chunk, int lastUnclosedChunkPoint, int pathIdx,
      TsFileIOWriter mergeFileWriter, IPointReader unseqReader,
      IChunkWriter chunkWriter, TsFileResource currFile) throws IOException {

    int unclosedChunkPoint = lastUnclosedChunkPoint;
    boolean chunkModified = currMeta.getDeletedAt() > Long.MIN_VALUE;

    // no need to write the chunk to .merge file
    if (!fullMerge && lastUnclosedChunkPoint == 0 && !chunkTooSmall && !chunkOverflowed && !chunkModified) {
      unmergedChunkNum.incrementAndGet();
      mergeContext.getUnmergedChunkStartTimes().get(currFile).get(currMergingPaths.get(pathIdx))
          .add(currMeta.getStartTime());
      return 0;
    }

    // write SK to .merge.file without compressing
    if (fullMerge && lastUnclosedChunkPoint == 0 && !chunkTooSmall && !chunkOverflowed && !chunkModified) {
      synchronized (mergeFileWriter) {
        mergeFileWriter.writeChunk(chunk, currMeta);
      }
      mergeContext.incTotalPointWritten(currMeta.getNumOfPoints());
      mergeContext.incTotalChunkWritten();
      mergedChunkNum.incrementAndGet();
      return 0;
    }

    // 3.1 SK isn't overflowed, just uncompress and write sequence chunk
    if (!chunkOverflowed) {
      unclosedChunkPoint += MergeUtils.writeChunkWithoutUnseq(chunk, chunkWriter);
      mergedChunkNum.incrementAndGet();
    } else {
      // 3.2 SK is overflowed, uncompress sequence chunk and merge with unseq chunk, then write
      unclosedChunkPoint += writeChunkWithUnseq(chunk, chunkWriter, unseqReader, pathIdx);
      mergedChunkNum.incrementAndGet();
    }

    // update points written statistics
    mergeContext.incTotalPointWritten(unclosedChunkPoint - lastUnclosedChunkPoint);
    if (minChunkPointNum > 0 && unclosedChunkPoint >= minChunkPointNum
        || unclosedChunkPoint > 0 && minChunkPointNum < 0) {
      // the new chunk's size is large enough and it should be flushed
      synchronized (mergeFileWriter) {
        chunkWriter.writeToFileWriter(mergeFileWriter);
        mergeContext.incTotalChunkWritten();
      }
      unclosedChunkPoint = 0;
    }
    return unclosedChunkPoint;
  }

  private int writeRemainingUnseq(IChunkWriter chunkWriter,
      IPointReader unseqReader,  int pathIdx) throws IOException {
    int ptWritten = 0;
    while (currTimeValuePairs[pathIdx] != null) {
      writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
      ptWritten++;
      unseqReader.next();
      currTimeValuePairs[pathIdx] = unseqReader.hasNext() ? unseqReader.current() : null;
    }
    return ptWritten;
  }

  private int writeChunkWithUnseq(Chunk chunk, IChunkWriter chunkWriter, IPointReader unseqReader, int pathIdx) throws IOException {
    int cnt = 0;
    ChunkReader chunkReader = new ChunkReader(chunk, null);
    while (chunkReader.hasNextSatisfiedPage()) {
      BatchData batchData = chunkReader.nextPageData();
      cnt += mergeWriteBatch(batchData, chunkWriter, unseqReader, pathIdx);
    }
    return cnt;
  }

  private int mergeWriteBatch(BatchData batchData, IChunkWriter chunkWriter,
      IPointReader unseqReader, int pathIdx) throws IOException {
    int cnt = 0;
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
        cnt++;
      }
      // unseq point.time > sequence point.time, write seq point
      if (!overwriteSeqPoint) {
        writeBatchPoint(batchData, i, chunkWriter);
        cnt++;
      }
    }
    return cnt;
  }
}
