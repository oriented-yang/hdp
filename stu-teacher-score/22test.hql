-- 22、查询所有课程的成绩第2名到第3名的学生信息及该课程成绩:

select
   s1.sid,
   s2.name,
   s2.birthday,
   c.name,
   s1.score,
   s1.rank
from (
  select
    cid,
    sid,
    score,
    row_number() over(partition by cid order by score desc) rank
  from score
) s1 
left join course c on s1.cid = c.id
left join student s2 on s1.sid = s2.id
where s1.rank between 2 and 3
