-- 18、查询各科成绩最高分、最低分和平均分：
-- 以如下形式显示：课程ID，课程name，最高分，最低分，平均分，及格率，中等率，优良率，优秀率:
-- 及格为>=60，中等为：70-80，优良为：80-90，优秀为：>=90

select
  t1.cid,
  t1.max_score,
  t1.min_score,
  round(t1.avg_score, 2) avg_score,
  if(round(t2.jgs / t1.total_rs, 2) is null, 0, round(t2.jgs / t1.total_rs, 2)) jgl,
  if(round(t3.zds / t1.total_rs, 2) is null, 0, round(t3.zds / t1.total_rs, 2)) zdl,
  if(round(t4.yls / t1.total_rs, 2) is null, 0, round(t4.yls / t1.total_rs, 2)) yll,
  if(round(t5.yxs / t1.total_rs, 2) is null, 0, round(t5.yxs / t1.total_rs, 2)) yxl
from (
  select
     s1.cid,
     max(s1.score) max_score,
     min(s1.score) min_score,
     avg(s1.score) avg_score,
     count(1) total_rs
  from score s1
  group by s1.cid
) t1
left join (
  select 
     s2.cid, 
     count(1) jgs 
  from score s2 
  where s2.score >= 60 
  group by s2.cid
) t2 on t1.cid = t2.cid
left join (
  select 
    s3.cid,
    count(1) zds 
  from score s3 
  where s3.score >= 70 and s3.score < 80 
  group by s3.cid
) t3 on t1.cid = t3.cid
left join (
  select 
    s4.cid,
    count(1) yls 
  from score s4 
  where s4.score >= 80 and s4.score < 90 
  group by s4.cid
) t4 on t1.cid = t4.cid
left join (
  select 
    s5.cid,
    count(1) yxs 
  from score s5 
  where s5.score >= 90 
  group by s5.cid
) t5 on t1.cid = t5.cid

