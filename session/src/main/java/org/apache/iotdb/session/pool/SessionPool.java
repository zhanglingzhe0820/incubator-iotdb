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
package org.apache.iotdb.session.pool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Config;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SessionPool is a wrapper of a Session Set. Using SessionPool, the user do not need to consider
 * how to reuse a session connection. Even if the session is disconnected, the session pool can
 * recognize it and remove the broken session connection and create a new one.
 * <p>
 * If there is no available connections and the pool reaches its max size, the all methods will hang
 * until there is a available connection.
 * <p>
 * If a user has waited for a session for more than 60 seconds, a warn log will be printed.
 * <p>
 * The only thing you have to remember is that:
 * <p>
 * For a query, if you have get all data, i.e., SessionDataSetWrapper.hasNext() == false, it is ok.
 * Otherwise, i.e., you want to stop the query before you get all data
 * (SessionDataSetWrapper.hasNext() == true), then you have to call closeResultSet(SessionDataSetWrapper
 * wrapper) manually. Otherwise the connection is occupied by the query.
 * <p>
 * Another case that you have to manually call closeResultSet() is that when there is exception when
 * you call SessionDataSetWrapper.hasNext() or next()
 */
public class SessionPool {

  private static final Logger logger = LoggerFactory.getLogger(SessionPool.class);
  private static int RETRY = 3;
  private ConcurrentLinkedDeque<Session> queue = new ConcurrentLinkedDeque<>();
  //for session whose resultSet is not released.
  private ConcurrentMap<Session, Session> occupied = new ConcurrentHashMap<>();
  private int size = 0;
  private int maxSize = 0;
  private String ip;
  private int port;
  private String user;
  private String password;
  private int fetchSize;
  private long timeout; //ms
  private static int FINAL_RETRY = RETRY - 1;
  private boolean enableCompression = false;

  public SessionPool(String ip, int port, String user, String password, int maxSize) {
    this(ip, port, user, password, maxSize, Config.DEFAULT_FETCH_SIZE, 60_000, false);
  }

  public SessionPool(String ip, int port, String user, String password, int maxSize,
      boolean enableCompression) {
    this(ip, port, user, password, maxSize, Config.DEFAULT_FETCH_SIZE, 60_000, enableCompression);
  }

  @SuppressWarnings("squid:S107")
  public SessionPool(String ip, int port, String user, String password, int maxSize, int fetchSize,
      long timeout, boolean enableCompression) {
    this.maxSize = maxSize;
    this.ip = ip;
    this.port = port;
    this.user = user;
    this.password = password;
    this.fetchSize = fetchSize;
    this.timeout = timeout;
    this.enableCompression = enableCompression;
  }

  //if this method throws an exception, either the server is broken, or the ip/port/user/password is incorrect.
  //TODO: we can add a mechanism that if the user waits too long time, throw exception.
  private Session getSession() throws IoTDBConnectionException {
    Session session = queue.poll();
    if (session != null) {
      return session;
    } else {
      synchronized (this) {
        long start = System.currentTimeMillis();
        while (session == null) {
          if (size < maxSize) {
            //we can create more session
            size++;
            //but we do it after skip synchronized block because connection a session is time consuming.
            break;
          } else {
            //we have to wait for someone returns a session.
            try {
              this.wait(1000);
              if (System.currentTimeMillis() - start > 60_000) {
                logger.warn(
                    "the SessionPool has wait for {} seconds to get a new connection: {}:{} with {}, {}",
                    (System.currentTimeMillis() - start) / 1000, ip, port, user, password);
                if (System.currentTimeMillis() - start > timeout) {
                  throw new IoTDBConnectionException(
                      String.format("timeout to get a connection from %s:%s", ip, port));
                }
              }
            } catch (InterruptedException e) {
              logger.error("the SessionPool is damaged", e);
              Thread.currentThread().interrupt();
            }
            session = queue.poll();
          }
        }
        if (session != null) {
          return session;
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Create a new Session {}, {}, {}, {}", ip, port, user, password);
      }
      session = new Session(ip, port, user, password, fetchSize);
      session.open(enableCompression);
      return session;
    }
  }

  public int currentAvailableSize() {
    return queue.size();
  }

  public int currentOccupiedSize() {
    return occupied.size();
  }

  private void putBack(Session session) {
    queue.push(session);
    synchronized (this) {
      this.notifyAll();
    }
  }

  private void occupy(Session session) {
    occupied.put(session, session);
  }

  /**
   * close all connections in the pool
   */
  public synchronized void close() {
    for (Session session : queue) {
      try {
        session.close();
      } catch (IoTDBConnectionException e) {
        //do nothing
      }
    }
    for (Session session : occupied.keySet()) {
      try {
        session.close();
      } catch (IoTDBConnectionException e) {
        //do nothing
      }
    }
    queue.clear();
    occupied.clear();
  }

  public void closeResultSet(SessionDataSetWrapper wrapper) {
    boolean putback = true;
    try {
      wrapper.sessionDataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      removeSession();
      putback = false;
    } finally {
      Session session = occupied.remove(wrapper.session);
      if (putback && session != null) {
        putBack(wrapper.session);
      }
    }
  }

  private synchronized void removeSession() {
    if (logger.isDebugEnabled()) {
      logger.debug("Remove a broken Session {}, {}, {}, {}", ip, port, user, password);
    }
    size--;
  }

  private void closeSession(Session session) {
    if (session != null) {
      try {
        session.close();
      } catch (Exception e2) {
        //do nothing. We just want to guarantee the session is closed.
      }
    }
  }

  private void cleanSessionAndMayThrowConnectionException(Session session, int times, IoTDBConnectionException e) throws IoTDBConnectionException {
    closeSession(session);
    removeSession();
    if (times == FINAL_RETRY) {
      throw new IoTDBConnectionException(
          String.format("retry to execute statement on %s:%s failed %d times: %s", ip, port,
              RETRY, e.getMessage()), e);
    }
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   *  a Tablet example:
   *
   *        device1
   *     time s1, s2, s3
   *     1,   1,  1,  1
   *     2,   2,  2,  2
   *     3,   3,  3,  3
   *
   * times in Tablet may be not in ascending order
   *
   * @param tablet data batch
   */
  public void insertTablet(Tablet tablet)
      throws IoTDBConnectionException, BatchExecutionException {
    insertTablet(tablet, false);
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * a Tablet example:
   *
   *      device1
   * time s1, s2, s3
   * 1,   1,  1,  1
   * 2,   2,  2,  2
   * 3,   3,  3,  3
   *
   * Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   *
   * @param tablet a tablet data of one device
   * @param sorted whether times in Tablet are in ascending order
   */
  public void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, BatchExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertTablet(tablet, sorted);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (BatchExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }


  /**
   * use batch interface to insert data
   *
   * @param tablets multiple batch
   */
  public void insertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, BatchExecutionException {
    insertTablets(tablets, false);
  }

  /**
   * use batch interface to insert data
   *
   * @param tablets multiple batch
   */
  public void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, BatchExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertTablets(tablets, sorted);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (BatchExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * Insert data in batch format, which can reduce the overhead of network. This method is just like
   * jdbc batch insert, we pack some insert request in batch and send them to server If you want
   * improve your performance, please see insertTablet method
   *
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList) throws IoTDBConnectionException, BatchExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (BatchExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }


  /**
   * Insert data in batch format, which can reduce the overhead of network. This method is just like
   * jdbc batch insert, we pack some insert request in batch and send them to server If you want
   * improve your performance, please see insertTablet method
   *
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList)
      throws IoTDBConnectionException, BatchExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRecords(deviceIds, times, measurementsList, valuesList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (BatchExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertRecords method
   * or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRecord(deviceId, time, measurements, types, values);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertRecords method
   * or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(String deviceId, long time, List<String> measurements,
      List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRecord(deviceId, time, measurements, values);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablet(Tablet tablet)
      throws IoTDBConnectionException, BatchExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertTablet(tablet);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (BatchExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, BatchExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertTablets(tablets);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (BatchExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList)
      throws IoTDBConnectionException, BatchExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertRecords(deviceIds, times, measurementsList, valuesList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (BatchExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, BatchExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertRecords(deviceIds, times, measurementsList, typesList, valuesList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (BatchExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecord(String deviceId, long time, List<String> measurements,
      List<String> values) throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertRecord(deviceId, time, measurements, values);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values) throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertRecord(deviceId, time, measurements, types, values);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param path timeseries to delete, should be a whole path
   */
  public void deleteTimeseries(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteTimeseries(path);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param paths timeseries to delete, should be a whole path
   */
  public void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteTimeseries(paths);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * delete data <= time in one timeseries
   *
   * @param path data in which time series to delete
   * @param time data with time stamp less than or equal to time will be deleted
   */
  public void deleteData(String path, long time)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteData(path, time);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * delete data <= time in multiple timeseries
   *
   * @param paths data in which time series to delete
   * @param time  data with time stamp less than or equal to time will be deleted
   */
  public void deleteData(List<String> paths, long time)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteData(paths, time);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void setStorageGroup(String storageGroupId)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.setStorageGroup(storageGroupId);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteStorageGroup(storageGroup);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void deleteStorageGroups(List<String> storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteStorageGroups(storageGroup);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void createTimeseries(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor) throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.createTimeseries(path, dataType, encoding, compressor);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void createTimeseries(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props, Map<String, String> tags,
      Map<String, String> attributes, String measurementAlias)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.createTimeseries(path, dataType, encoding, compressor, props, tags, attributes,
            measurementAlias);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes,
      List<TSEncoding> encodings, List<CompressionType> compressors,
      List<Map<String, String>> propsList, List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList, List<String> measurementAliasList)
      throws IoTDBConnectionException, BatchExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.createMultiTimeseries(paths, dataTypes, encodings, compressors, propsList, tagsList,
            attributesList, measurementAliasList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (BatchExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public boolean checkTimeseriesExists(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        boolean resp = session.checkTimeseriesExists(path);
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
    //never go here.
    return false;
  }

  /**
   * execure query sql users must call closeResultSet(SessionDataSetWrapper) if they do not use the
   * SessionDataSet any more. users do not need to call sessionDataSet.closeOpeationHandler() any
   * more.
   *
   * @param sql query statement
   * @return result set Notice that you must get the result instance. Otherwise a data leakage will
   * happen
   */
  public SessionDataSetWrapper executeQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        SessionDataSet resp = session.executeQueryStatement(sql);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
    // never go here
    return null;
  }

  /**
   * execute non query statement
   *
   * @param sql non query statement
   */
  public void executeNonQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.executeNonQueryStatement(sql);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException e) {
        putBack(session);
        throw e;
      }
    }
  }
}
