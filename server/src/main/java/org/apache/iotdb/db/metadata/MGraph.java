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
package org.apache.iotdb.db.metadata;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StorageGroupException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * Metadata Graph consists of one {@code MTree} and several {@code PTree}.
 */
public class MGraph {

  private static final String ESCAPED_SEPARATOR = "\\.";
  private static final String TIME_SERIES_INCORRECT = "Timeseries's root is not Correct. RootName: ";
  private MTree mtree;
  private HashMap<String, PTree> ptreeMap;

  MGraph(String mtreeName) {
    mtree = new MTree(mtreeName);
    ptreeMap = new HashMap<>();
  }

  /**
   * Add a {@code PTree} to current {@code MGraph}.
   */
  void addAPTree(String ptreeRootName) throws MetadataErrorException {
    if (IoTDBConstant.PATH_ROOT.equalsIgnoreCase(ptreeRootName)) {
      throw new MetadataErrorException("Property Tree's root name should not be 'root'");
    }
    PTree ptree = new PTree(ptreeRootName, mtree);
    ptreeMap.put(ptreeRootName, ptree);
  }

  /**
   * Add a seriesPath to Metadata Tree.
   *
   * @param path Format: root.node.(node)*
   */
  public void addPathToMTree(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws PathErrorException {
    String[] nodes = MetaUtils.getNodeNames(path);
    mtree.addTimeseriesPath(nodes, dataType, encoding, compressor, props);
  }

  /**
   * Add a deviceId to Metadata Tree.
   */
  MNode addDeviceIdToMTree(String deviceId) throws PathErrorException {
    return mtree.addDeviceId(deviceId);
  }

  /**
   * Add a seriesPath to {@code PTree}.
   */
  void addPathToPTree(String path) throws PathErrorException {
    String[] nodes = MetaUtils.getNodeNames(path);

    String rootName = nodes[0];
    if (ptreeMap.containsKey(rootName)) {
      PTree ptree = ptreeMap.get(rootName);
      ptree.addPath(nodes);
    } else {
      throw new PathErrorException(TIME_SERIES_INCORRECT + rootName);
    }
  }

  /**
   * Delete seriesPath in current MGraph.
   *
   * @param path a seriesPath belongs to MTree or PTree
   */
  String deletePath(String path) throws PathErrorException {
    String[] nodes = MetaUtils.getNodeNames(path);

    String rootName = nodes[0];
    if (mtree.getRoot().getName().equals(rootName)) {
      return mtree.deletePath(path);
    } else if (ptreeMap.containsKey(rootName)) {
      PTree ptree = ptreeMap.get(rootName);
      ptree.deletePath(path);
      return null;
    } else {
      throw new PathErrorException(TIME_SERIES_INCORRECT + rootName);
    }
  }

  /**
   * Link a {@code MNode} to a {@code PNode} in current PTree.
   */
  void linkMNodeToPTree(String pPath, String mPath) throws PathErrorException {
    String[] pNodes = MetaUtils.getNodeNames(pPath);
    String pRootName = pNodes[0];
    if (!ptreeMap.containsKey(pRootName)) {
      throw new PathErrorException("Error: PTree Path Not Correct. Path: " + pPath);
    } else {
      ptreeMap.get(pRootName).linkMNode(pNodes, mPath);
    }
  }

  /**
   * Unlink a {@code MNode} from a {@code PNode} in current PTree.
   */
  void unlinkMNodeFromPTree(String pPath, String mPath) throws PathErrorException {
    String[] pNodes = MetaUtils.getNodeNames(pPath);
    String pRootName = pNodes[0];
    if (!ptreeMap.containsKey(pRootName)) {
      throw new PathErrorException("Error: PTree Path Not Correct. Path: " + pPath);
    } else {
      ptreeMap.get(pRootName).unlinkMNode(pNodes, mPath);
    }
  }

  /**
   * Set storage group for current Metadata Tree.
   *
   * @param path Format: root.node.(node)*
   */
  void setStorageGroup(String path) throws StorageGroupException {
    mtree.setStorageGroup(path);
  }

  /**
   * Delete storage group from current Metadata Tree.
   *
   * @param path Format: root.node
   */
  void deleteStorageGroup(String path) throws PathErrorException {
    mtree.deleteStorageGroup(path);
  }

  /**
   * Check whether the input path is a storage group in current Metadata Tree or not.
   *
   * @param path Format: root.node.(node)*
   * @apiNote :for cluster
   */
  boolean checkStorageGroup(String path) {
    return mtree.checkStorageGroup(path);
  }

  /**
   * Get all paths for given seriesPath regular expression if given seriesPath belongs to MTree, or
   * get all linked seriesPath for given seriesPath if given seriesPath belongs to PTree Notice:
   * Regular expression in this method is formed by the amalgamation of seriesPath and the character
   * '*'.
   *
   * @return A HashMap whose Keys are the storage group names.
   */
  HashMap<String, List<String>> getAllPathGroupByStorageGroup(String path)
      throws PathErrorException {
    String[] nodes = MetaUtils.getNodeNames(path);
    String rootName = nodes[0];
    if (mtree.getRoot().getName().equals(rootName)) {
      return mtree.getAllPath(nodes);
    } else if (ptreeMap.containsKey(rootName)) {
      PTree ptree = ptreeMap.get(rootName);
      return ptree.getAllLinkedPath(path);
    }
    throw new PathErrorException(TIME_SERIES_INCORRECT + rootName);
  }

  List<MNode> getAllStorageGroupNodes() {
    return mtree.getAllStorageGroupNodes();
  }

  /**
   * function for getting all timeseries paths under the given seriesPath.
   * @return each list in the returned list consists of 4 strings: full path, storage group, data
   *   type and encoding of a timeseries
   */
  List<List<String>> getTimeseriesInfo(String path) throws PathErrorException {
    String[] nodes = MetaUtils.getNodeNames(path);
    String rootName = nodes[0];
    if (mtree.getRoot().getName().equals(rootName)) {
      return mtree.getTimeseriesInfo(nodes);
    } else if (ptreeMap.containsKey(rootName)) {
      throw new PathErrorException(
          "PTree is not involved in the execution of the sql 'show timeseries " + path + "'");
    }
    throw new PathErrorException(TIME_SERIES_INCORRECT + rootName);
  }

  List<String> getAllStorageGroupNames() {
    return mtree.getAllStorageGroupList();
  }

  List<String> getAllDevices() {
    return mtree.getAllDevices();
  }

  List<String> getNodesList(String prefixPath, int nodeLevel) throws SQLException {
    return mtree.getNodesList(prefixPath, nodeLevel);
  }

  List<String> getLeafNodePathInNextLevel(String path) throws PathErrorException {
    return mtree.getLeafNodePathInNextLevel(path);
  }

  /**
   * Get all ColumnSchemas for the storage group seriesPath.
   *
   * @param path the Path in a storage group
   * @return List<'   ColumnSchema   '> The list of the schema
   */
  List<MeasurementSchema> getSchemaInOneStorageGroup(String path) {
    return mtree.getSchemaForOneStorageGroup(path);
  }

  Map<String, MeasurementSchema> getSchemaMapInStorageGroup(String path) {
    return mtree.getSchemaMapForOneStorageGroup(path);
  }

  Map<String, Integer> getNumSchemaMapInStorageGroup(String path) {
    return mtree.getNumSchemaMapForOneFileNode(path);
  }

  /**
   * Get the file name for given seriesPath Notice: This method could be called if and only if the
   * seriesPath includes one node whose {@code isStorageGroup} is true.
   */
  String getStorageGroupNameByPath(String path) throws StorageGroupException {
    return mtree.getStorageGroupNameByPath(path);
  }

  String getStorageGroupNameByPath(MNode node, String path) throws StorageGroupException {
    return mtree.getStorageGroupNameByPath(node, path);
  }

  boolean checkStorageGroupByPath(String path) {
    return mtree.checkFileNameByPath(path);
  }

  /**
   * Get all file names for given seriesPath
   */
  List<String> getAllStorageGroupNamesByPath(String path) throws PathErrorException {
    return mtree.getAllFileNamesByPath(path);
  }

  /**
   * Check whether the seriesPath given exists.
   */
  boolean pathExist(String path) {
    return mtree.isPathExist(path);
  }

  /**
   *
   * @param path
   * @return an MNode corresponding to path
   * @throws PathErrorException if the path does not exist or does not belong to a storage group.
   */
  MNode getNodeInStorageGroup(String path) throws PathErrorException {
      return mtree.getNodeInStorageGroup(path);
  }

  /**
   * Get MeasurementSchema for given seriesPath. Notice: Path must be a complete Path from root to leaf
   * node.
   */
  MeasurementSchema getSchemaForOnePath(String path) throws PathErrorException {
    return mtree.getSchemaForOnePath(path);
  }

  /**
   * functions for converting the mTree to a readable string in json format.
   */
  @Override
  public String toString() {
    return mtree.toString();
  }

  /**
   * @return storage group name -> the series number
   */
  Map<String, Integer> countSeriesNumberInEachStorageGroup() {
    Map<String, Integer> res = new HashMap<>();
    List<MNode> storageGroups = getAllStorageGroupNodes();
    for (MNode sg : storageGroups) {
      res.put(sg.getFullPath(), sg.getLeafCount());
    }
    return res;
  }
}
