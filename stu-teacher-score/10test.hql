--10、查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息:

select
  s1.sid,
  stu.name,
  s1.cid,
  s1.score,
  s2.cid,
  s2.score
from (
  select 
    ss.sid sid,
    ss.cid cid,
    ss.score score
  from score ss 
  where ss.sid not in (
      select 
   	distinct s.sid 
      from score s where s.cid='02'
  )
) s1 
left join score s2 on s1.sid = s2.sid
left join student stu on s1.sid = stu.id
where s1.cid = '01'
and s1.cid <> s2.cid
