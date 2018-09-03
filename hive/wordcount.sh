create table hive_wordcount(context string);
show tables;
# 加载数据到hive表
load data local inpath '/home/hadoop/data/test.txt' into table hive_wordcount;
select * from hive_wordcount;
# 分组查询
select word, count(1) from hive_wordcount lateral view explode(split(context, '\t')) wc as word group by word;