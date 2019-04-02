create database sparksql_test;
use sparksql_test;

create table student(id string,name string,birthday string,sex string) row format delimited fields terminated by '\t';
create table course(id string,name string,tid string) row format delimited fields terminated by '\t';
create table teacher(id string,name string) row format delimited fields terminated by '\t';
create table score(sid string,cid string,score int) row format delimited fields terminated by '\t';

load data local inpath '/home/bigdata/data/sparksql/stu-teacher-score/student.csv' into table student;
load data local inpath '/home/bigdata/data/sparksql/stu-teacher-score/course.csv' into table course;
load data local inpath '/home/bigdata/data/sparksql/stu-teacher-score/teacher.csv' into table teacher;
load data local inpath '/home/bigdata/data/sparksql/stu-teacher-score/score.csv' into table score;

