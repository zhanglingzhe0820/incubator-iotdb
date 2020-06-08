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
package org.apache.iotdb.db.engine.merge.seqMerge.squeeze.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.seqMerge.squeeze.recover.SqueezeMergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.LeafMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqueezeMergeTask implements Callable<Void> {

  public static final String MERGE_SUFFIX = ".merge.squeeze";
  private static final Logger logger = LoggerFactory.getLogger(SqueezeMergeTask.class);

  MergeResource resource;
  String storageGroupSysDir;
  String storageGroupName;
  private SqueezeMergeLogger mergeLogger;
  private MergeContext mergeContext = new MergeContext();

  MergeCallback callback;
  String taskName;

  List<TsFileResource> newResources;

  public SqueezeMergeTask(
      MergeResource mergeResource, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    this.resource = mergeResource;
    this.storageGroupSysDir = storageGroupSysDir;
    this.callback = callback;
    this.taskName = taskName;
    this.storageGroupName = storageGroupName;
  }

  @Override
  public Void call() throws Exception {
    try {
      doMerge();
    } catch (Exception e) {
      logger.error("Runtime exception in merge {}", taskName, e);
      cleanUp(false);
      // call the callback to make sure the StorageGroup exit merging status, but passing 2
      // empty file lists to avoid files being deleted.
      callback.call(
          Collections.emptyList(), Collections.emptyList(),
          SystemFileFactory.INSTANCE.getFile(storageGroupSysDir,
              SqueezeMergeLogger.MERGE_LOG_NAME), null);
      throw e;
    }
    return null;
  }

  private void doMerge() throws IOException, MetadataException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} seqFiles, {} unseqFiles", taskName,
          resource.getSeqFiles().size(), resource.getUnseqFiles().size());
    }
    long startTime = System.currentTimeMillis();
    long totalFileSize = MergeUtils.collectFileSizes(resource.getSeqFiles(),
        resource.getUnseqFiles());
    mergeLogger = new SqueezeMergeLogger(storageGroupSysDir);

    mergeLogger.logFiles(resource);

    Set<String> devices = MManager.getInstance().getDevices(storageGroupName);
    Map<Path, IChunkWriter> chunkWriterCacheMap = new HashMap<>();
    for (String device : devices) {
      InternalMNode deviceNode = (InternalMNode) MManager.getInstance().getNodeByPath(device);
      for (Entry<String, MNode> entry : deviceNode.getChildren().entrySet()) {
        MeasurementSchema measurementSchema = ((LeafMNode) entry.getValue()).getSchema();
        chunkWriterCacheMap
            .put(new Path(device, entry.getKey()), new ChunkWriterImpl(measurementSchema));
      }
    }
    resource.setChunkWriterCache(chunkWriterCacheMap);

    List<String> storageGroupPaths = MManager.getInstance()
        .getAllTimeseriesName(storageGroupName + ".*");
    List<Path> unmergedSeries = new ArrayList<>();
    for (String path : storageGroupPaths) {
      unmergedSeries.add(new Path(path));
    }

    MergeSeriesTask mergeChunkTask = new MergeSeriesTask(mergeContext, taskName, mergeLogger,
        resource, unmergedSeries);
    newResources = mergeChunkTask.mergeSeries();

    cleanUp(true);
    if (logger.isInfoEnabled()) {
      double elapsedTime = (double) (System.currentTimeMillis() - startTime) / 1000.0;
      double byteRate = totalFileSize / elapsedTime / 1024 / 1024;
      double seriesRate = unmergedSeries.size() / elapsedTime;
      double fileRate =
          (resource.getSeqFiles().size() + resource.getUnseqFiles().size()) / elapsedTime;
      double ptRate = mergeContext.getTotalPointWritten() / elapsedTime;
      logger.info("{} ends after {}s, byteRate: {}MB/s, seriesRate {}/s, "
              + "fileRate: {}/s, ptRate: {}/s",
          taskName, elapsedTime, byteRate, seriesRate, fileRate, ptRate);
    }
  }

  void cleanUp(boolean executeCallback) throws IOException {
    logger.info("{} is cleaning up", taskName);

    resource.clear();
    mergeContext.clear();

    if (mergeLogger != null) {
      mergeLogger.close();
    }

    for (TsFileResource seqFile : resource.getSeqFiles()) {
      deleteFile(seqFile);
    }
    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      deleteFile(unseqFile);
    }

    File logFile = FSFactoryProducer.getFSFactory().getFile(storageGroupSysDir,
        SqueezeMergeLogger.MERGE_LOG_NAME);
    if (executeCallback) {
      // make sure merge.log is not deleted until unseqFiles are cleared so that when system
      // reboots, the undeleted files can be deleted again
      callback.call(resource.getSeqFiles(), resource.getUnseqFiles(), logFile, newResources);
    } else {
      logFile.delete();
    }
  }

  private void deleteFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      resource.removeFileReader(seqFile);
      ChunkMetadataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getPath());
      seqFile.getFile().delete();
      File resourceFile = new File(seqFile.getPath() + MERGE_SUFFIX);
      resourceFile.delete();
      seqFile.setMerging(false);
      seqFile.setDeleted(true);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }
}