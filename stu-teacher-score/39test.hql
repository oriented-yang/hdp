-- 39、求每门课程的学生人数:
select
   s.cid,
   c.name,
   count(1) countz
from course c
left join score s on c.id = s.cid
group by s.cid, c.name

