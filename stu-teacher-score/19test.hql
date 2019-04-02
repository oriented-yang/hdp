-- 19、按各科成绩进行排序，并显示排名:
select
  cid,
  sid,
  score,
  row_number() over(partition by cid order by score desc) rank
from score
