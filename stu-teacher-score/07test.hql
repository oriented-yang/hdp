-- 7、查询学过"张三"老师授课的同学的信息:
select
   s2.id,
   s2.name,
   s2.birthday,
   c.name,
   s1.score
from score s1
left join course c on s1.cid = c.id
left join teacher t on t.id = c.tid
left join student s2 on s1.sid = s2.id
where t.name='张三'
