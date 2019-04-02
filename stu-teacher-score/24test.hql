-- 24、查询学生平均成绩及其名次:

select
  t.sid,
  t.avg_score,
  row_number() over(order by t.avg_score desc) rank
from (
  select
     sid,
     round(avg(score),2) avg_score
  from score
  group by sid
) t
