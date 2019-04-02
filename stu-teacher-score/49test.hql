-- 49、查询本月过生日的学生:
select
  id,
  name,
  birthday
from student
where month(birthday)=month(current_date())
