<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# 第6章: 系统工具
# 加载外部tsfile文件
<!-- TOC -->

- [第6章: 系统工具](#第6章-系统工具)
- [加载外部tsfile文件](#加载外部tsfile文件)
- [介绍](#介绍)
- [使用方式](#使用方式)
    - [加载tsfile文件](#加载tsfile文件)
    - [删除tsfile文件](#删除tsfile文件)
    - [移出tsfile文件至指定目录](#移出tsfile文件至指定目录)

<!-- /TOC -->
# 介绍
加载外部tsfile文件工具允许用户向正在运行中的Apache IoTDB中加载、删除或移出tsfile文件。

# 使用方式
用户通过Cli工具或JDBC向Apache IoTDB系统发送指定命令实现文件加载的功能。
## 加载tsfile文件
加载tsfile文件的指令为：`load <path/dir> [true/false] [storage group level]`

该指令有两种用法：
1. 通过指定文件路径(绝对路径)加载单tsfile文件。
第二个参数表示待加载的tsfile文件的路径，其中文件名称需要符合tsfile的命名规范，即`{systemTime}-{versionNum}-{mergeNum}.tsfile`。第三、四个参数为可选项。当待加载的tsfile文件中时间序列对应的元数据不存在时，用户可以选择是否自动创建schema，参数为true表示自动创建schema，相反false表示不创建，缺省时默认创建schema。当tsfile对应的存储组不存在时，用户可以通过第四个参数来制定存储组的级别，默认为`iotdb-engine.properties`中设置的级别。若待加载的tsfile文件对应的`.resource`文件存在，会被一并加载至Apache IoTDB数据文件的目录和引擎中，否则将通过tsfile文件重新生成对应的`.resource`文件，即加载的tsfile文件所对应的`.resource`文件不是必要的。
示例：
    * `load /Users/Desktop/data/1575028885956-101-0.tsfile`
    * `load /Users/Desktop/data/1575028885956-101-0.tsfile false`
    * `load /Users/Desktop/data/1575028885956-101-0.tsfile true`
    * `load /Users/Desktop/data/1575028885956-101-0.tsfile true 2`


2. 通过指定文件夹路径(绝对路径)批量加载文件。
第二个参数表示待加载的tsfile文件的路径，其中文件名称需要符合tsfile的命名规范，即`{systemTime}-{versionNum}-{mergeNum}.tsfile`。第三、四个参数为可选项。当待加载的tsfile文件中时间序列对应的元数据不存在时，用户可以选择是否自动创建schema，参数为true表示自动创建schema，相反false表示不创建，缺省时默认创建schema。当tsfile对应的存储组不存在时，用户可以通过第四个参数来制定存储组的级别，默认为`iotdb-engine.properties`中设置的级别。若待加载文件对应的`.resource`文件存在，则会一并加载至Apache IoTDB数据文件目录和引擎中，否则将通过tsfile文件重新生成对应的`.resource`文件，即加载的tsfile文件所对应的`.resource`文件不是必要的。
示例：
    * `load /Users/Desktop/data`
    * `load /Users/Desktop/data false`
    * `load /Users/Desktop/data true`
    * `load /Users/Desktop/data true 2`