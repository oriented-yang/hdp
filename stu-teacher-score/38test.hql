--38、查询课程编号为01且课程成绩在80分以上的学生的学号和姓名:

select
   s1.sid,
   s2.name,
   s1.cid,
   s1.score
from score s1
left join student s2 on s1.sid = s2.id
where s1.cid='01' and s1.score >= 80
