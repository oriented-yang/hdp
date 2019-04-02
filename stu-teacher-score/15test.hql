--15、查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩:
select 
  s.sid,
  stu.name,
  s.countz,
  round(s.avg_score, 2) avg_score
from (
  select
    s1.sid,
    count(s1.score) countz,
    avg(s1.score) avg_score
  from score s1
  where s1.cid < 60
  group by s1.sid
) s
left join student stu on s.sid = stu.id
where s.countz >= 2
