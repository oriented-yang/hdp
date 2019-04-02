-- 11、查询没有学全所有课程的同学的信息:

select
  tmp.sid,
  stu.name
from (
  select
     t1.sid
  from (
    select
      s1.sid,
      count(s1.cid) countz
    from score s1
    group by s1.sid
  ) t1
  left join (
    select count(1) total_course from course
  ) t2
  where t1.countz < t2.total_course
) tmp 
left join student stu on tmp.sid = stu.id
