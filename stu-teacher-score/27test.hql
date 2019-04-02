-- 27、查询出只有两门课程的全部学生的学号和姓名:

select
   s1.sid,
   s2.name  
from (
  select
    sid,
    count(1) countz
  from score
  group by sid
) s1
left join student s2 on s1.sid = s2.id
where s1.countz = 2
