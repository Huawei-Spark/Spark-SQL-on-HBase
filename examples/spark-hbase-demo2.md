## create spark sql table map to existing hbase (only single column mapping to hbase rowkey is supported)
(1) create table in hbase
```
create 'hbase10k', 'f'
for i in '1'..'10000' do for j in '1'..'2' do put 'hbase10k', "row#{i}", "f:c#{j}", "#{i}#{j}" end end
```   

(2) relate 'test' from SparkOnHbase
```
CREATE TABLE spark10k(rowkey STRING, a INTEGER, b INTEGER, PRIMARY KEY (rowkey)) MAPPED BY (hbase10k, COLS=[a=f.c1, b=f.c2]);
```

(3) Query :
```
   // test count *
   (1) select count(*) from spark10k

   // test group by
   (2) select avg(a), b from spark10k group by b
```