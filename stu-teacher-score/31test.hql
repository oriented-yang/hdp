-- 31、查询1990年出生的学生名单:

select
 *
from student
where year(birthday) = '1990'
