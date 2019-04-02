-- 9、查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息:

select
    s1.sid,
    s.name,
    s1.cid,
    s1.score,
    s2.cid,
    s2.score
from score s1
join score s2 on s1.sid = s2.sid
left join student s on s1.sid = s.id
where s1.cid='01' and s2.cid='02'
