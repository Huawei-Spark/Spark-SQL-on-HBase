#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.sql import SQLContext
from py4j.java_gateway import java_import

def register(sc):
    java_import(sc._gateway.jvm, "org.apache.spark.sql.hbase.HBaseSQLContext")

__all__ = ["HBaseSQLContext"]

class HBaseSQLContext(SQLContext):
    """A variant of Spark SQL that integrates with data stored in HBase.
    """

    def __init__(self, sparkContext):
        """Create a new HbaseContext.

    @param sparkContext: The SparkContext to wrap.
    """
        SQLContext.__init__(self, sparkContext)
        self._scala_HBaseSQLContext = self._get_hbase_ctx()

    @property
    def _ssql_ctx(self):
        if self._scala_HBaseSQLContext is None:
            print ("loading hbase context ..")
            self._scala_HBaseSQLContext = self._get_hbase_ctx()
        if self._scala_SQLContext is None:
            self._scala_SQLContext = self._scala_HBaseSQLContext
        return self._scala_HBaseSQLContext

    def _get_hbase_ctx(self):
        return self._jvm.HBaseSQLContext(self._jsc.sc())

#TODO: add tests if for main
