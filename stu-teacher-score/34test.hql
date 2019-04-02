-- 34、查询课程名称为"数学"，且分数低于60的学生姓名和分数:

select
  c.name,
  s2.name,
  s1.score
from course c
left join score s1 on c.id = s1.cid
left join student s2 on s1.sid = s2.id
where c.name='数学'
