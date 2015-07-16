# Spark SQL on HBase

Apache HBase is a distributed Key-Value store of data on HDFS. It is modeled after Google’s Big Table, and provides APIs to query the data. The data is organized, partitioned and distributed by its “row keys”. Per partition, the data is further physically partitioned by “column families” that specify collections of “columns” of data. The data model is for wide and sparse tables where columns are dynamic and may well be sparse.

Although HBase is a very useful big data store, its access mechanism is very primitive and only through client-side APIs, Map/Reduce interfaces and interactive shells. SQL accesses to HBase data are available through Map/Reduce or interfaces mechanisms such as Apache Hive and Impala, or some “native” SQL technologies like Apache Phoenix. While the former is usually cheaper to implement and use, their latencies and efficiencies often cannot compare favorably with the latter and are often suitable only for offline analysis. The latter category, in contrast, often performs better and qualifies more as online engines; they are often on top of purpose-built execution engines.

Currently Spark supports queries against HBase data through HBase’s Map/Reduce interface (i.e., TableInputFormat). Spark SQL supports use of Hive data, which theoretically should be able to support HBase data access, out-of-box, through HBase’s Map/Reduce interface and therefore falls into the first category of the “SQL on HBase” technologies.

We believe, as a unified big data processing engine, Spark is in good position to provide better HBase support.

## Online Documentation

Online documentation https://github.com/Huawei-Spark/Spark-SQL-on-HBase/blob/master/doc/SparkSQLOnHBase_v2.2.docx

## Requirements

This version of 1.0.0 requires Spark 1.4.0.

## Building Spark HBase

Spark HBase is built using [Apache Maven](http://maven.apache.org/).


I. Clone and build Huawei-Spark/Spark-SQL-on-HBase

    $ git clone https://github.com/Huawei-Spark/Spark-SQL-on-HBase spark-hbase
```
II. Go to the root of the source tree
```
    $ cd spark-hbase
```
III. Build without testing
```
    $ mvn -Phbase,hadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests clean package install
```
IV. Build and run test suites against a HBase minicluster, from Maven.
```
    $ mvn clean install
```

## Interactive Scala Shell

The easiest way to start using Spark HBase is through the Scala shell:

    ./bin/hbase-sql


## Python Shell

First, add the spark-hbase jar to the SPARK_CLASSPATH in the $SPARK_HOME/conf directory, as follows:

SPARK_CLASSPATH=$SPARK_CLASSPATH:/spark-hbase-root-dir/target/spark-hbase_2.10-1.4.0-SNAPSHOT.jar
   

Then go to the spark-hbase installation directory and issue

   ./bin/pyspark-hbase

A successfull message is as follows:

   You are using Spark SQL on HBase!!!
   HBaseSQLContext available as hsqlContext.

To run a python script, the PYTHONPATH environment should be set to the "python" directory of the Spark-HBase installation. For example,

    export PYTHONPATH=/root-of-Spark-HBase/python

## Running Tests

Testing first requires [building Spark HBase](#building-spark). Once Spark HBase is built ...

Run all test suites from Maven:

    mvn -Phbase,hadoop-2.4 test

Run a single test suite from Maven, for example:

    mvn -Phbase,hadoop-2.4 test -DwildcardSuites=org.apache.spark.sql.hbase.BasicQueriesSuite

## IDE Setup

We use IntelliJ IDEA for Spark HBase development. You can get the community edition for free and install the JetBrains Scala plugin from Preferences > Plugins.

To import the current Spark HBase project for IntelliJ:

1. Download IntelliJ and install the Scala plug-in for IntelliJ. You may also need to install Maven plug-in for IntelliJ.
2. Go to "File -> Import Project", locate the Spark HBase source directory, and select "Maven Project".
3. In the Import Wizard, select "Import Maven projects automatically" and leave other settings at their default. 
4. Make sure some specific profiles are enabled. Select corresponding Hadoop version, "maven3" and also"hbase" in order to get dependencies.
5. Leave other settings at their default and you should be able to start your development.
6. When you run the scala test, sometimes you will get out of memory exception. You can increase your VM memory usage by the following setting, for example:

```
    -XX:MaxPermSize=512m -Xmx3072m
```

You can also make those setting to be the default by setting to the "Defaults -> ScalaTest".

## Configuration

Please refer to the [Configuration guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.
