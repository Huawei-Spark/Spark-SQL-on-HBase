## Create and query SparkSQL table map to HBase table (multiple columns map to hbase table rowkey)
In this example, we create a new SparkSQL table and map it to a new HBase table with multiple column in rowkey.

Steps:
(1) Create table in hbase-sql shell:
```
$SPARK_HBASE_Home/bin/hbase-sql
CREATE TABLE teacher1k(grade int, class int, subject string, teacher_name string, teacher_age int, PRIMARY KEY (grade, class, subject)) MAPPED BY (hbase1k, COLS=[teacher_name=teacher.name, teacher_age=teacher.age]);
```

This command will create following tables:
Tables :
  spark :  teacher1k
  hbase :  hbase1k
  
Fields :
  [grade,int]
  [class,int]
  [subject,string]
  [teacher_name,string]
  [teacher_age,int]

  key columns : grade,class,subject
  non-key colums: teacher_name, teacher_age
  
(2) Load data from a csv data file:
```
LOAD DATA INPATH './examples/teacher1k.csv' INTO TABLE teacher1k FIELDS TERMINATED BY "," ;
```

(3) Query :
```
    // test where
    (1) select teacher_name,teacher_age from teacher1k where teacher_age > 25;

    // test like in
    (2) select teacher_name,teacher_age,subject from teacher1k where teacher_name is not null and teacher_name like 'teacher_2_3%' and teacher_age not in (20,21,22,23,24,25)

    // test subquery
    (3) select t1.teacher_name,t1.teacher_age from (select * from teacher1k where teacher_name like 'teacher_2_3%') t1 where t1.teacher_age < 25

    //test group by
    (4) select teacher_name, sum(teacher_age) from teacher1k where grade=1 group by teacher_name

    //test join
    (5) select t1.teacher_name, t2.subject, t1.teacher_age from (select teacher_name, teacher_age from teacher1k where teacher_age >=26 ) t1 join  (select teacher_name, subject from teacher1k where teacher_name like 'teacher_2_3%')t2 on t1.teacher_name=t2.teacher_name
```
