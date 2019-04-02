-- 21、查询不同老师所教不同课程平均分从高到低显示:

select
   c.tid,
   t2.name,
   c.name,
   t1.cid_avg
from (
  select
    s1.cid,
    round(avg(s1.score),2) cid_avg
  from score s1
  group by s1.cid
) t1
left join course c on t1.cid = c.id
left join teacher t2 on c.tid = t2.id
order by t1.cid_avg desc
