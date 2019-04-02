-- 17、按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩:
select
  s1.sid,
  s2.name,
  sum(s1.score) total_score,
  avg(s1.score) avg_score
from score s1
left join student s2 on s1.sid = s2.id
group by s1.sid, s2.name
order by avg_score desc
