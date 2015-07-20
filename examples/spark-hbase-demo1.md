## Create and query SparkSQL table map to HBase (support multiple columns mapping to hbase rowkey)
(1)TableName :
  spark :  teacher1k
  hbase :  hbase1k
  
(2)Fields :
  [grade,int]
  [class,int]
  [subject,string]
  [teacher_name,string]
  [teacher_age,int]

  keyCols : grade,class,subject

(3) Create table:
```
CREATE TABLE teacher1k(grade int, class int, subject string, teacher_name string, teacher_age int, PRIMARY KEY (grade, class, subject)) MAPPED BY (hbase1k, COLS=[teacher_name=teacher.name, teacher_age=teacher.age]);
```

(4) Load data :
```
LOAD DATA INPATH './examples/teacher1k.csv' INTO TABLE teacher1k FIELDS TERMINATED BY "," ;
```

(5) Query :
```
    // test where
    (1) select teacher_name,teacher_age from teacher1k where teacher_age > 25;

    // test like in
    (2) select teacher_name,teacher_age,subject from teacher1k where teacher_name is not null and teacher_name like 'teacher_2_3%' and teacher_age not in (20,21,22,23,24,25)

    // test subquery
    (3) select t1.teacher_name,t1.teacher_age from (select * from teacher1k where teacher_name like 'teacher_2_3%') t1 where t1.teacher_age < 25

    //test group
    (4) select teacher_name, sum(teacher_age) from teacher1k where grade=1 group by teacher_name

    //test join
    (5) select t1.teacher_name, t2.subject, t1.teacher_age from (select teacher_name, teacher_age from teacher1k where teacher_age >=26 ) t1 join  (select teacher_name, subject from teacher1k where teacher_name like 'teacher_2_3%')t2 on t1.teacher_name=t2.teacher_name
```
