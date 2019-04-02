--  26、查询每门课程被选修的学生数:

select
  cid,
  count(1) rs
from score
group by cid
