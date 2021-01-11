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

package org.apache.iotdb.db.engine.cache;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

public class FileStatisticsCache {

  private final AtomicLong cacheHitNum = new AtomicLong();
  private final AtomicLong cacheRequestNum = new AtomicLong();
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private static LRULinkedHashMap<AccountableString, FileStatistics> fileStatisticsCache;
  private static final long MEMORY_THRESHOLD_IN_FILE_STATISTICS_CACHE = IoTDBDescriptor
      .getInstance()
      .getConfig().getAllocateMemoryForFileStatisticsCache();

  private FileStatisticsCache() {
    fileStatisticsCache = new LRULinkedHashMap<AccountableString, FileStatistics>(
        MEMORY_THRESHOLD_IN_FILE_STATISTICS_CACHE) {
      @Override
      protected long calEntrySize(AccountableString key, FileStatistics value) {
        if (value == null) {
          return RamUsageEstimator.sizeOf(key);
        }
        long entrySize;
        if (count < 10) {
          long currentSize = value.calculateRamSize();
          averageSize = ((averageSize * count) + currentSize) / (++count);
          entrySize = RamUsageEstimator.sizeOf(key)
              + (currentSize + RamUsageEstimator.NUM_BYTES_OBJECT_REF)
              + RamUsageEstimator.shallowSizeOf(value);
        } else if (count < 100000) {
          count++;
          entrySize = RamUsageEstimator.sizeOf(key)
              + (averageSize + RamUsageEstimator.NUM_BYTES_OBJECT_REF)
              + RamUsageEstimator.shallowSizeOf(value);
        } else {
          averageSize = value.calculateRamSize();
          count = 1;
          entrySize = RamUsageEstimator.sizeOf(key)
              + (averageSize + RamUsageEstimator.NUM_BYTES_OBJECT_REF)
              + RamUsageEstimator.shallowSizeOf(value);
        }
        return entrySize;
      }
    };
  }

  public void put(String filePath, long totalPoint, int sensorNum) {
    lock.writeLock().lock();
    fileStatisticsCache
        .put(new AccountableString(filePath), new FileStatistics(totalPoint, sensorNum));
    lock.writeLock().unlock();
  }

  public FileStatistics get(TsFileResource fileResource) throws IOException {
    String filePath = fileResource.getTsFilePath();
    AccountableString key = new AccountableString(filePath);
    cacheRequestNum.incrementAndGet();
    if (fileStatisticsCache.containsKey(key)) {
      cacheHitNum.incrementAndGet();
      lock.readLock().lock();
      try {
        return fileStatisticsCache.get(key);
      } finally {
        lock.readLock().unlock();
      }
    } else {
      lock.writeLock().lock();
      try (TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader(
          fileResource.getTsFilePath())) {
        long totalPoints = 0;
        Set<String> sensorSet = new HashSet<>();
        List<String> devices = tsFileSequenceReader.getAllDevices();
        for (String device : devices) {
          Map<String, List<ChunkMetadata>> chunkMetadataListMap = tsFileSequenceReader
              .readChunkMetadataInDevice(device);
          for (List<ChunkMetadata> chunkMetadataList : chunkMetadataListMap.values()) {
            for (ChunkMetadata chunkMetadata : chunkMetadataList) {
              totalPoints += chunkMetadata.getNumOfPoints();
              sensorSet.add(chunkMetadata.getMeasurementUid());
            }
          }
        }
        FileStatistics fileStatistics = new FileStatistics(totalPoints, sensorSet.size());
        fileStatisticsCache.put(key, fileStatistics);
        return fileStatistics;
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  /**
   * clear cache.
   */
  public void clear() {
    if (fileStatisticsCache != null) {
      fileStatisticsCache.clear();
    }
  }

  public void remove(String filePath) {
    if (fileStatisticsCache != null) {
      fileStatisticsCache.remove(new AccountableString(filePath));
    }
  }

  public double calculateFileStatisticsHitRatio() {
    if (cacheRequestNum.get() != 0) {
      return cacheHitNum.get() * 1.0 / cacheRequestNum.get();
    } else {
      return 0;
    }
  }

  public long getUsedMemory() {
    return fileStatisticsCache.getUsedMemory();
  }

  public long getMaxMemory() {
    return fileStatisticsCache.getMaxMemory();
  }

  public double getUsedMemoryProportion() {
    return fileStatisticsCache.getUsedMemoryProportion();
  }

  public long getAverageSize() {
    return fileStatisticsCache.getAverageSize();
  }


  public static FileStatisticsCache getInstance() {
    return FileStatisticsCacheHolder.INSTANCE;
  }

  /**
   * singleton pattern.
   */
  private static class FileStatisticsCacheHolder {

    private static final FileStatisticsCache INSTANCE = new FileStatisticsCache();
  }
}
