--48、查询下周过生日的学生:
select
   id,
   name,
   birthday
from student
where weekofyear(birthday) = (weekofyear(current_date()) + 1)
