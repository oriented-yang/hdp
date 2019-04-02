-- 1、查询"01"课程比"02"课程成绩高的学生的信息及课程分数:
select
   s.id,
   s.name,
   s.birthday,
   s.sex,
   t1.score
from (
    select
        s1.sid,
        s1.score
    from score s1 join score s2 on s1.sid = s2.sid
    where s1.cid = '01' and s2.cid = '02' and s1.score > s2.score
) t1
left join student s on t1.sid = s.id;
