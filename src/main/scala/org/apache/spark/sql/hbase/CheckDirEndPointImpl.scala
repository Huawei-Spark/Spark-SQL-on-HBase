/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.sql.hbase

import com.google.protobuf.{RpcCallback, RpcController, Service}
import org.apache.hadoop.hbase.coprocessor._
import org.apache.hadoop.hbase.{Coprocessor, CoprocessorEnvironment}
import org.apache.log4j.Logger

/*When we enable coprocessor and codegen at the same time,
* if the current directory of any regionservers is not accessible,
* it will crash the HBase regionserver service!!!
*
* This issue normally happens in CDH service.
* Comparing to standalone HBase service, hbase-regionserver, the start script of CDH,
* doesn't include 'cd $HBASE_HOME', which might lead to the result that
* system doesn't regard '.' as a directory.
*
* Thus, we use this endpoint service to check whether
* the current directory is accessible or not in advance.
* If it is not, we will not use codegen.
*
* For CDH service, the better solution is adding 'cd'
* to hbase-regionserver in /etc/init.d
*/
class CheckDirEndPointImpl
  extends CheckDirProtos.CheckDirService with Coprocessor with CoprocessorService {

  private lazy val logger = Logger.getLogger(getClass.getName)
  private var env: RegionCoprocessorEnvironment = null

  override def start(env: CoprocessorEnvironment) = {
    if (!env.isInstanceOf[RegionCoprocessorEnvironment]) {
      throw new CoprocessorException("Must be loaded on a table region!")
    }
  }

  override def stop(env: CoprocessorEnvironment) = {}

  override def getService: Service = this

  override def getCheckResult(controller: RpcController,
                              request: CheckDirProtos.CheckRequest,
                              done: RpcCallback[CheckDirProtos.CheckResponse]) = {
    val isDir = new java.io.File(".").isDirectory
    if (!isDir) {
      logger.warn(
        """Current directory is not accessible,
          |please add 'cd ~' before start regionserver in your regionserver start script.""")
    }
    val response: CheckDirProtos.CheckResponse = {
      CheckDirProtos.CheckResponse.newBuilder().setAccessible(isDir).build()
    }
    done.run(response)
  }
}
