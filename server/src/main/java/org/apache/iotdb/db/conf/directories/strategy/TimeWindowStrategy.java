/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.conf.directories.strategy;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.utils.CommonUtils;

/**
 * TimeWindowStrategy switches the directory periodically, making it possible to merge and
 * write on different disks.
 */
public class TimeWindowStrategy extends DirectoryStrategy {

  private long timeUnit;
  private int indexOffset;

  public TimeWindowStrategy() {
    this.indexOffset = 0;
    this.timeUnit = IoTDBDescriptor.getInstance().getConfig().getWindowDirStrategyTimeUnit();
  }

  @Override
  public int nextFolderIndex() throws DiskSpaceInsufficientException {
    long currTime = System.currentTimeMillis();
    int startIndex = (int) ((currTime / timeUnit + indexOffset) % folders.size());
    for (int i = 0; i < folders.size(); i++) {
      int index = (startIndex + i) % folders.size();
      if (CommonUtils.hasSpace(folders.get(index))) {
        return index;
      }
    }
    throw new DiskSpaceInsufficientException(
        String.format("All disks of folders %s are full, can't proceed.", folders));
  }

  public void setIndexOffset(int indexOffset) {
    this.indexOffset = indexOffset;
  }
}