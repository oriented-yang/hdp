-- 20、查询学生的总成绩并进行排名:
select
  sid,
  sum(score) sum
from score
group by sid
order by sum
