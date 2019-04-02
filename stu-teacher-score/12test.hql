-- 12、查询至少有一门课与学号为"01"的同学所学相同的同学的信息:

select
  distinct s2.sid,
  stu.name,
  stu.birthday,
  stu.sex
from (
  select
    cid
  from score s1
  where sid='01'
) t1
right join score s2 on t1.cid = s2.cid
left join student stu on s2.sid = stu.id
where s2.sid <> '01'
