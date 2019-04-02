--23、统计各科成绩各分数段人数：课程编号,课程名称,[100-85],[85-70],[70-60],[0-60]及所占百分比
  
select
  t1.cid,
  c.name,
  round(t1.avg_score, 2) avg_score,
  if(round(t2.bjgs / t1.total_rs, 2) is null, 0, round(t2.bjgs / t1.total_rs, 2)) bjgl,
  if(round(t3.zds / t1.total_rs, 2) is null, 0, round(t3.zds / t1.total_rs, 2)) zdl,
  if(round(t4.yls / t1.total_rs, 2) is null, 0, round(t4.yls / t1.total_rs, 2)) yll,
  if(round(t5.yxs / t1.total_rs, 2) is null, 0, round(t5.yxs / t1.total_rs, 2)) yxl
from (
  select
     s1.cid,
     avg(s1.score) avg_score,
     count(1) total_rs
  from score s1
  group by s1.cid
) t1
left join (
  select 
     s2.cid, 
     count(1) bjgs 
  from score s2 
  where s2.score < 60 
  group by s2.cid
) t2 on t1.cid = t2.cid
left join (
  select 
    s3.cid,
    count(1) zds 
  from score s3 
  where s3.score >= 60 and s3.score < 70 
  group by s3.cid
) t3 on t1.cid = t3.cid
left join (
  select 
    s4.cid,
    count(1) yls 
  from score s4 
  where s4.score >= 70 and s4.score < 85 
  group by s4.cid
) t4 on t1.cid = t4.cid
left join (
  select 
    s5.cid,
    count(1) yxs 
  from score s5 
  where s5.score >= 85 
  group by s5.cid
) t5 on t1.cid = t5.cid
left join course c on t1.cid = c.id
