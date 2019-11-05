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

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR_NO_REGEX;

import org.apache.iotdb.db.exception.PathErrorException;

class MetaUtils {

  private MetaUtils() {
    // util class
  }

  static String[] getNodeNames(String path) throws PathErrorException {
    path = path.trim();

    int firstSingleQuotePos = -1;
    int firstDoubleQuotePos = -1;
    for (int i = 0; i < path.length(); i++) {
      if (firstSingleQuotePos == -1 && path.charAt(i) == '\'') {
        firstSingleQuotePos = i;
      }
      if (firstDoubleQuotePos == -1 && path.charAt(i) == '\"') {
        firstDoubleQuotePos = i;
      }
    }

    if (firstDoubleQuotePos != -1 && firstSingleQuotePos != -1) {
      throw new PathErrorException("Path contains both \" and \': " + path);
    }

    return getNodeNames(path, firstSingleQuotePos, firstDoubleQuotePos);
  }

  private static String[] getNodeNames(String path, int firstSingleQuotePos,
      int firstDoubleQuotePos) throws PathErrorException {
    String[] nodeNames;

    if (firstSingleQuotePos != -1 && path.charAt(path.length() - 1) != '\'' ||
        firstDoubleQuotePos != -1 && path.charAt(path.length() - 1) != '\"') {
      throw new PathErrorException("Path contains but not ends with \' or \": " + path);
    } else if (firstDoubleQuotePos == -1 && firstSingleQuotePos == -1) {
      return path.split(PATH_SEPARATOR_NO_REGEX);
    }

    String device = null;
    String measurement;
    int quotePos = firstDoubleQuotePos != -1 ? firstDoubleQuotePos :
        firstSingleQuotePos;
    if (quotePos == 0) {
      measurement = path;
    } else {
      device = path.substring(0, quotePos - 1);
      measurement = path.substring(quotePos + 1, path.length() - 1);
    }

    if (device == null) {
      nodeNames = new String[]{measurement};
    } else {
      String[] deviceNodeNames = device.split(PATH_SEPARATOR_NO_REGEX);
      int nodeNumber = deviceNodeNames.length + 1;
      nodeNames = new String[nodeNumber];
      System.arraycopy(deviceNodeNames, 0, nodeNames, 0, nodeNumber - 1);
      nodeNames[nodeNumber - 1] = measurement;
    }

    return nodeNames;
  }
}
