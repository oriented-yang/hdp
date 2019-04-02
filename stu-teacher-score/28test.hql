-- 28、查询男生、女生人数:

select
  sex,
  count(1) rs
from student
group by sex
