ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE kozhagulovra;

-- Найти наиболее прибыльный день месяца для каждого налогоплательщика
WITH cte AS (
    SELECT 
        content.userInn as userInn, 
        DAY(FROM_UNIXTIME(CAST(content.dateTime.ddate AS BIGINT) DIV 1000)) as day,
        COALESCE(content.totalSum, 0) as totalSum
    FROM kkt_transactions
    WHERE content.userInn IS NOT NULL 
        AND content.dateTime.ddate IS NOT NULL
),
cte2 AS (
    SELECT 
        userInn, 
        day, 
        SUM(totalSum) as sum
    FROM cte
    GROUP BY userInn, day
),
cte3 AS (
    SELECT 
        userInn, 
        day, 
        sum, 
        ROW_NUMBER() OVER (PARTITION BY userInn ORDER BY sum DESC) as rn
    FROM cte2
)
SELECT 
    userInn, 
    day, 
    sum
FROM cte3
WHERE rn = 1
ORDER BY CAST(userInn AS BIGINT) ASC;