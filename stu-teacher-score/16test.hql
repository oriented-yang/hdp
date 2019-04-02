-- 16、检索"01"课程分数小于60，按分数降序排列的学生信息:

select
  s1.sid,
  s2.name,
  s1.score
from score s1
left join student s2 on s1.sid = s2.id
where s1.cid='01'
and s1.score < 60
order by s1.score desc

