-- 5、查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩:

select
   s1.sid,
   s2.name,
   count(distinct s1.cid) num_course,
   sum(s1.score) total_score
from score s1
left join student s2 on s1.sid = s2.id
group by s1.sid, s2.name
