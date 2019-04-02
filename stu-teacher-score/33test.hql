-- 33、查询平均成绩大于等于85的所有学生的学号、姓名和平均成绩:

select
   s1.sid,
   s2.name,
   round(avg(s1.score), 2) avg_score
from score s1
left join student s2 on s1.sid = s2.id
group by s1.sid, s2.name



