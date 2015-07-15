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

"""
An interactive shell.

This file is designed to be launched as a PYTHONSTARTUP script.
"""

import atexit
import os
import platform

import py4j

import pyspark
from pyspark_hbase.sql import HBaseSQLContext, context

print("You are using Spark SQL on HBase!!!")
try:
    context.register(sc)
    hsqlContext = HBaseSQLContext(sc)
except py4j.protocol.Py4JError:
    print("HBaseSQLContext can not be instantiated, falling back to SQLContext now")
    hsqlContext = SQLContext(sc)
except TypeError:
    print("HBaseSQLContext can not be instantiated, falling back to SQLContext now")
    hsqlContext = SQLContext(sc)

print("%s available as hsqlContext." % hsqlContext.__class__.__name__)
