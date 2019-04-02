--  40、查询选修"张三"老师所授课程的学生中，成绩最高的学生信息及其成绩:

select
   t.name,
   c.name,
   s2.name,
   s1.score
from teacher t
left join course c on t.id = c.tid
left join score s1 on c.id = s1.cid
left join student s2 on s1.sid = s2.id
where t.name='张三'
order by s1.score desc
limit 1
