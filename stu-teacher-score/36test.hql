--36、查询任何一门课程成绩在70分以上的学生姓名、课程名称和分数:
select
   s1.sid,
   s2.name,
   s1.score
from score s1
left join student s2 on s1.sid = s2.id
where s1.score > 70






