--45、查询选修了全部课程的学生信息:

select
   t1.sid,
   s2.name,
   s2.birthday
from (select
  s1.sid,
  count(1) countz
from score s1
group by s1.sid
) t1
left join (
  select 
     count(1) countz
  from course 
) t2 on t1.countz = t2.countz 
left join student s2 on t1.sid = s2.id
