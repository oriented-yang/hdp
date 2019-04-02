--44、检索至少选修两门课程的学生学号:

select
  s1.sid,
  count(1) countz
from score s1
group by s1.sid
having countz >= 2
