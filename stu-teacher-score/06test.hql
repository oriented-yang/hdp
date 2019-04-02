-- 6、查询"李"姓老师的数量:

select
   count(1) countz
from teacher
where name like '李%'
