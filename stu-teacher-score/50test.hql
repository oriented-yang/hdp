--50、查询12月份过生日的学生:
select
  id,
  name,
  birthday
from student
where month(birthday)=12
