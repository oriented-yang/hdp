--37、查询课程不及格的学生

select
   distinct sid
from score 
where score < 60
