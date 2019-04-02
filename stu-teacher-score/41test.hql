-- 41、查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩:

select
   s1.sid,
   s3.name,
   s1.cid,
   s1.score,
   s2.cid,
   s2.score
from score s1
join score s2 on s1.sid = s2.sid
left join student s3 on s1.sid = s3.id
where s1.score = s2.score and s1.cid <> s2.cid
