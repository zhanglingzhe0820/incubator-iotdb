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

package org.apache.iotdb.db.engine.merge.strategy.overlapped.squeeze.recover;

import java.util.Collection;
import java.util.List;
import org.apache.iotdb.db.engine.merge.IRecoverMergeTask;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.squeeze.task.SqueezeAppendMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

/**
 * RecoverMergeTask is an extension of MergeTask, which resumes the last merge progress by scanning
 * merge.log using LogAnalyzer and continue the unfinished merge.
 */
public class RecoverSqueezeAppendMergeTask extends SqueezeAppendMergeTask implements IRecoverMergeTask {

  public RecoverSqueezeAppendMergeTask(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName) {
    super(new MergeResource(seqFiles, unseqFiles), storageGroupSysDir, callback, taskName,
        storageGroupName);
  }

  // continueMerge does not work for squeeze strategy
  public void recoverMerge(boolean continueMerge) {
  }

  private void removeMergedFile(List<TsFileResource> tsFileResources) {
  }
}
