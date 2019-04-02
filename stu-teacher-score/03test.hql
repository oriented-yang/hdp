-- 3、查询平均成绩大于等于60分的同学的学生编号和学生姓名和平均成绩:

select
   tmp.sid,
   tmp.name,
   round(tmp.avg_score) avg_score
from (
    select
       s1.sid,
       s2.name,
       avg(s1.score) avg_score
    from score s1
    left join student s2 on s1.sid = s2.id
    group by s1.sid, s2.name
) tmp
where tmp.avg_score > 60
