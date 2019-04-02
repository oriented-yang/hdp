--46、查询各学生的年龄(周岁):
--    按照出生日期来算，当前月日 < 出生年月的月日则，年龄减一


select
   id,
   birthday,
   current_date() c_date,
   if(cast(date_format(current_date(), 'Md') as int) < cast(date_format(birthday, 'Md') as int), (year(current_date()) - year(birthday) - 1), year(current_date()) - year(birthday)) age
from student
