-- 25、查询各科成绩前三名的记录

select
  tmp.*
from (
  select
    cid,
    score,
    row_number() over(partition by cid order by score desc) rank
  from score
) tmp 
where rank < 4
