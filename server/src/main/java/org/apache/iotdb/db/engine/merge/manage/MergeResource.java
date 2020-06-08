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

package org.apache.iotdb.db.engine.merge.manage;

import static org.apache.iotdb.db.engine.merge.seqMerge.inplace.task.InplaceMergeTask.MERGE_SUFFIX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.reader.resource.CachedUnseqResourceMergeReader;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

/**
 * MergeResource manages files and caches of readers, writers, MeasurementSchemas and modifications
 * to avoid unnecessary object creations and file openings.
 */
public class MergeResource {

  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles;

  private Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();
  private Map<TsFileResource, RestorableTsFileIOWriter> fileWriterCache = new HashMap<>();
  private Map<TsFileResource, List<Modification>> modificationCache = new HashMap<>();
  private Map<Path, IChunkWriter> chunkWriterCache = new ConcurrentHashMap<>();

  private boolean cacheDeviceMeta = false;

  public MergeResource() {
    this.seqFiles = new ArrayList<>();
    this.unseqFiles = new ArrayList<>();
  }

  public MergeResource(List<TsFileResource> seqFiles) {
    this(seqFiles, new ArrayList<>());
  }

  public MergeResource(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
  }

  public void clear() throws IOException {
    for (TsFileSequenceReader sequenceReader : fileReaderCache.values()) {
      sequenceReader.close();
    }
    for (RestorableTsFileIOWriter writer : fileWriterCache.values()) {
      writer.close();
    }

    fileReaderCache.clear();
    fileWriterCache.clear();
    modificationCache.clear();
    chunkWriterCache.clear();
  }

  public IChunkWriter getChunkWriter(Path path) {
    return chunkWriterCache.get(path);
  }

  /**
   * Construct a new or get an existing RestorableTsFileIOWriter of a merge temp file for a SeqFile.
   * The path of the merge temp file will be the seqFile's + ".merge".
   *
   * @return A RestorableTsFileIOWriter of a merge temp file for a SeqFile.
   */
  public RestorableTsFileIOWriter getMergeFileWriter(TsFileResource resource) throws IOException {
    RestorableTsFileIOWriter writer = fileWriterCache.get(resource);
    if (writer == null) {
      writer = new RestorableTsFileIOWriter(FSFactoryProducer.getFSFactory()
          .getFile(resource.getPath() + MERGE_SUFFIX));
      fileWriterCache.put(resource, writer);
    }
    return writer;
  }

  /**
   * Query ChunkMetadata of a timeseries from the given TsFile (seq or unseq). The ChunkMetadata is
   * not cached since it is usually huge.
   *
   * @param path name of the time series
   */
  public List<ChunkMetadata> queryChunkMetadata(Path path, TsFileResource seqFile)
      throws IOException {
    TsFileSequenceReader sequenceReader = getFileReader(seqFile);
    return sequenceReader.getChunkMetadataList(path);
  }

  /**
   * Query all ChunkMetadata of a file
   */
  public List<ChunkMetadata> queryChunkMetadata(TsFileResource seqFile)
      throws IOException {
    TsFileSequenceReader sequenceReader = getFileReader(seqFile);
    List<Path> paths = sequenceReader.getAllPaths();
    List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
    for (Path path : paths) {
      chunkMetadataList.addAll(sequenceReader.getChunkMetadataList(path));
    }
    return chunkMetadataList;
  }

  /**
   * Construct the a new or get an existing TsFileSequenceReader of a TsFile.
   *
   * @return a TsFileSequenceReader
   */
  public TsFileSequenceReader getFileReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getPath(), true, cacheDeviceMeta);
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }

  /**
   * Construct UnseqResourceMergeReaders of for each timeseries over all unseqFiles. The readers are
   * not cached since the method is only called once for each timeseries.
   *
   * @param paths names of the timeseries
   * @return an array of UnseqResourceMergeReaders each corresponding to a timeseries in paths
   */
  public IPointReader[] getUnseqReaders(List<Path> paths) throws IOException {
    List<Chunk>[] pathChunks = MergeUtils.collectUnseqChunks(paths, unseqFiles, this);
    IPointReader[] ret = new IPointReader[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      TSDataType dataType = getChunkWriter(paths.get(i)).getDataType();
      ret[i] = new CachedUnseqResourceMergeReader(pathChunks[i], dataType);
    }
    return ret;
  }

  /**
   * Get the modifications of a timeseries in the ModificationFile of a TsFile. Once the
   * modifications of the timeseries are found out, they will be removed from the list to boost the
   * next query, so two calls of the same file and timeseries are forbidden.
   *
   * @param path name of the time series
   */
  public List<Modification> getModifications(TsFileResource tsFileResource, Path path) {
    // copy from TsFileResource so queries are not affected
    List<Modification> modifications = modificationCache.computeIfAbsent(tsFileResource,
        resource -> new LinkedList<>(resource.getModFile().getModifications()));
    List<Modification> pathModifications = new ArrayList<>();
    Iterator<Modification> modificationIterator = modifications.iterator();
    // each path is visited only once in a merge, so the modifications can be removed after visiting
    while (modificationIterator.hasNext()) {
      Modification modification = modificationIterator.next();
      if (modification.getPath().equals(path)) {
        pathModifications.add(modification);
        modificationIterator.remove();
      }
    }
    return pathModifications;
  }

  /**
   * Remove and close the writer of the merge temp file of a SeqFile. The merge temp file is also
   * deleted.
   *
   * @param tsFileResource the SeqFile
   */
  public void removeFileAndWriter(TsFileResource tsFileResource) throws IOException {
    RestorableTsFileIOWriter newFileWriter = fileWriterCache.remove(tsFileResource);
    if (newFileWriter != null) {
      newFileWriter.close();
      newFileWriter.getFile().delete();
    }
  }

  /**
   * Remove and close the reader of the TsFile. The TsFile is NOT deleted.
   *
   * @param resource the SeqFile
   */
  public void removeFileReader(TsFileResource resource) throws IOException {
    TsFileSequenceReader sequenceReader = fileReaderCache.remove(resource);
    if (sequenceReader != null) {
      sequenceReader.close();
    }
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  public void setSeqFiles(List<TsFileResource> seqFiles) {
    this.seqFiles = seqFiles;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  public void setUnseqFiles(
      List<TsFileResource> unseqFiles) {
    this.unseqFiles = unseqFiles;
  }

  public void removeOutdatedSeqReaders() throws IOException {
    Iterator<Entry<TsFileResource, TsFileSequenceReader>> entryIterator =
        fileReaderCache.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Entry<TsFileResource, TsFileSequenceReader> entry = entryIterator.next();
      TsFileResource tsFile = entry.getKey();
      if (!seqFiles.contains(tsFile)) {
        TsFileSequenceReader reader = entry.getValue();
        reader.close();
        entryIterator.remove();
      }
    }
  }

  public void setCacheDeviceMeta(boolean cacheDeviceMeta) {
    this.cacheDeviceMeta = cacheDeviceMeta;
  }

  public void setChunkWriterCache(Map<Path, IChunkWriter> chunkWriterCache) {
    this.chunkWriterCache = chunkWriterCache;
  }

  public void clearChunkWriterCache() {
    this.chunkWriterCache.clear();
  }

}
