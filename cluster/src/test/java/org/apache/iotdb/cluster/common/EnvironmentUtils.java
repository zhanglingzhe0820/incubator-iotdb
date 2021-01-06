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
package org.apache.iotdb.cluster.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.MetricsService;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This class is used for cleaning test environment in unit test and integration test
 * </p>
 */
public class EnvironmentUtils {

  private static final String OUTPUT_DATA_DIR = "target/";

  private EnvironmentUtils() {
    // util class
  }

  private static final Logger logger = LoggerFactory.getLogger(EnvironmentUtils.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static DirectoryManager directoryManager = DirectoryManager.getInstance();

  private static long testQueryId = 1;

  private static long oldTsFileThreshold = config.getTsFileSizeThreshold();

  private static long oldGroupSizeInByte = config.getMemtableSizeThreshold();

  public static void cleanEnv() throws IOException, StorageEngineException {
    System.out.println("Cleaning environment...");
    QueryResourceManager.getInstance().endQuery(testQueryId);

    // clear opened file streams
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

    // clean storage group manager
    if (!StorageEngine.getInstance().deleteAll()) {
      logger.error("Can't close the storage group manager in EnvironmentUtils");
      Assert.fail();
    }
    StorageEngine.getInstance().reset();
    IoTDBDescriptor.getInstance().getConfig().setReadOnly(false);

    StatMonitor.getInstance().close();
    // clean wal
    MultiFileLogNodeManager.getInstance().stop();
    // clean cache
    if (config.isMetaDataCacheEnable()) {
      ChunkMetadataCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
    }
    // close metadata
    IoTDB.metaManager.clear();

    MergeManager.getINSTANCE().stop();
    MetricsService.getInstance().stop();
    // delete all directory
    cleanAllDir();

    config.setTsFileSizeThreshold(oldTsFileThreshold);
    config.setMemtableSizeThreshold(oldGroupSizeInByte);
  }

  public static void cleanAllDir() throws IOException {
    // delete sequential files
    for (String path : directoryManager.getAllSequenceFileFolders()) {
      cleanDir(path);
    }
    // delete unsequence files
    for (String path : directoryManager.getAllUnSequenceFileFolders()) {
      cleanDir(path);
    }
    // delete system info
    cleanDir(config.getSystemDir());
    // delete wal
    cleanDir(config.getWalDir());
    // delete query
    cleanDir(config.getQueryDir());
    cleanDir("remote");
    // delete data files
    for (String dataDir : config.getDataDirs()) {
      cleanDir(dataDir);
    }
  }

  public static void cleanDir(String dir) throws IOException {
    deleteRecursively(new File(dir));
  }

  public static void deleteRecursively(File file) throws IOException {
    if (file.exists()) {
      if (file.isDirectory()) {
        File[] files = file.listFiles();
        if (files != null) {
          for (File child : files) {
            deleteRecursively(child);
          }
        }
      }
      try {
        Files.delete(Paths.get(file.getAbsolutePath()));
      } catch (DirectoryNotEmptyException e) {
        deleteRecursively(file);
      } catch (NoSuchFileException e) {
        // ignore;
      }
    }
  }


  /**
   * disable the system monitor</br> this function should be called before all code in the setup
   */
  public static void closeStatMonitor() {
    config.setEnableStatMonitor(false);
  }

  /**
   * disable memory control</br> this function should be called before all code in the setup
   */
  public static void envSetUp() throws StartupException {
    IoTDB.metaManager.init();

    createAllDir();
    // disable the system monitor
    config.setEnableStatMonitor(false);
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new StartupException(e);
    }
    try {
      authorizer.reset();
    } catch (AuthException e) {
      throw new StartupException(e);
    }
    StorageEngine.getInstance().reset();
    MultiFileLogNodeManager.getInstance().start();
    FlushManager.getInstance().start();
    MergeManager.getINSTANCE().start();
    testQueryId = QueryResourceManager.getInstance().assignQueryId(true, 1024, -1);
  }

  private static void createAllDir() {
    // create sequential files
    for (String path : directoryManager.getAllSequenceFileFolders()) {
      createDir(path);
    }
    // create unsequential files
    for (String path : directoryManager.getAllUnSequenceFileFolders()) {
      createDir(path);
    }
    // create storage group
    createDir(config.getSystemDir());
    // create wal
    createDir(config.getWalDir());
    // create query
    createDir(config.getQueryDir());
    createDir(OUTPUT_DATA_DIR);
    // create data
    for (String dataDir : config.getDataDirs()) {
      createDir(dataDir);
    }
  }

  private static void createDir(String dir) {
    File file = new File(dir);
    file.mkdirs();
  }
}
