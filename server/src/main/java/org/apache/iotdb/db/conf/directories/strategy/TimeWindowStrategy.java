/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.conf.directories.strategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.utils.CommonUtils;

/**
 * TimeWindowStrategy switches the directory periodically, making it possible to merge and
 * write on different disks.
 */
public class TimeWindowStrategy extends DirectoryStrategy {

  private long timeUnit;

  private List<String> nonFullFolders = new ArrayList<>();
  private List<String> dirsForInsert = null;
  private List<String> dirsForMerge = null;
  private int insertDirIdx = 0;
  private int mergeDirIdx = 0;

  private long lastCheckTimeInstance = 0;

  public TimeWindowStrategy() {
    this.timeUnit = IoTDBDescriptor.getInstance().getConfig().getWindowDirStrategyTimeUnit();
  }

  @Override
  public void init(List<String> folders) throws DiskSpaceInsufficientException {
    super.init(folders);


  }

  /**
   * If time has changed to next instance (time unit is 1 hour and time has changed from 8:00 to
   * 9:00), switch dirs for merge and insert.
   */
  private void checkSwitch() throws DiskSpaceInsufficientException {
    long currentTimeInstance = System.currentTimeMillis() / timeUnit;
    if (currentTimeInstance == lastCheckTimeInstance) {
      return;
    }

    lastCheckTimeInstance = currentTimeInstance;
    recalculateDir(currentTimeInstance);
  }

  private void recalculateDir(long currentTimeInstance) throws DiskSpaceInsufficientException {
    // re-calculate non-full folders since some previously full folders may be non-full after
    // a cycle
    nonFullFolders.clear();
    for (String folder : folders) {
      if (CommonUtils.hasSpace(folder)) {
        nonFullFolders.add(folder);
      }
    }
    if (nonFullFolders.isEmpty()) {
      throw new DiskSpaceInsufficientException("No available disk");
    }
    // divide the folders into ones for insertion and ones for merge
    if (nonFullFolders.size() == 1) {
      // we cannot divide if there is only one folder
      dirsForInsert = Collections.singletonList(nonFullFolders.get(0));
      dirsForMerge = Collections.singletonList(nonFullFolders.get(0));
      return;
    }
    // TODO: other division strategy
    int half = folders.size() / 2;
    List<String> firstHalf = folders.subList(0, half);
    List<String> secondHalf = folders.subList(half, folders.size());
    dirsForMerge = currentTimeInstance % 2 == 0 ? firstHalf : secondHalf;
    dirsForInsert = currentTimeInstance % 2 == 1 ? firstHalf : secondHalf;
  }

  @Override
  public int nextInsertFolderIndex() throws DiskSpaceInsufficientException {
    checkSwitch();
    for (int i = 0; i < dirsForInsert.size(); i++) {
      insertDirIdx = (insertDirIdx + i) % dirsForInsert.size();
      if (CommonUtils.hasSpace(dirsForInsert.get(insertDirIdx))) {
        return insertDirIdx;
      }
    }
    // some dirs for merge may still have space, try re-divide dirs
    recalculateDir(lastCheckTimeInstance);
    for (int i = 0; i < dirsForInsert.size(); i++) {
      insertDirIdx = (insertDirIdx + i) % dirsForInsert.size();
      if (CommonUtils.hasSpace(dirsForInsert.get(insertDirIdx))) {
        return insertDirIdx;
      }
    }
    throw new DiskSpaceInsufficientException(
        String.format("All disks of folders %s are full, can't proceed.", folders));
  }

  @Override
  List<String> getMergableDirs() throws DiskSpaceInsufficientException {
    checkSwitch();
    if (dirsForMerge.isEmpty()) {
      // some dirs for insert may still have space, try re-divide dirs
      recalculateDir(lastCheckTimeInstance);
      if (dirsForMerge.isEmpty()) {
        throw new DiskSpaceInsufficientException(
            String.format("All disks of folders %s are full, can't proceed.", folders));
      }
    }
    return dirsForMerge;
  }
}