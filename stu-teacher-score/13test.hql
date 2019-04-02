-- 13、查询和"01"号的同学学习的课程完全相同的其他同学的信息:
SELECT 
    DISTINCT A.sid 
FROM (
    SELECT 
        s1.sid,
        COUNT(*) count1 
    FROM score s1
    WHERE cid IN (
        SELECT 
            cid 
        FROM score s2 
        WHERE s2.sid='01'
    ) AND s1.sid <> '01'
    GROUP BY s1.sid
) A,(
SELECT 
    s3.sid,
    COUNT(*) count2 
FROM score s3
where s3.sid='01'
GROUP BY s3.sid
) B 
WHERE A.count1 = B.count2 
