 ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE kozhagulovra;

-- Найти налогоплательщиков с большей утренней прибылью чем вечерней
WITH morning_profits AS (
    SELECT 
        content.userInn as userInn,
        AVG(COALESCE(content.totalSum, 0)) as morning_avg_profit
    FROM kkt_transactions
    WHERE subtype = "receipt" 
        AND content.userInn IS NOT NULL 
        AND content.dateTime.ddate IS NOT NULL
        AND content.totalSum IS NOT NULL
        AND HOUR(FROM_UNIXTIME(CAST(content.dateTime.ddate AS BIGINT) DIV 1000)) < 13
    GROUP BY content.userInn
),
evening_profits AS (
    SELECT 
        content.userInn as userInn,
        AVG(COALESCE(content.totalSum, 0)) as evening_avg_profit
    FROM kkt_transactions
    WHERE subtype = "receipt" 
        AND content.userInn IS NOT NULL 
        AND content.dateTime.ddate IS NOT NULL
        AND content.totalSum IS NOT NULL
        AND HOUR(FROM_UNIXTIME(CAST(content.dateTime.ddate AS BIGINT) DIV 1000)) >= 13
    GROUP BY content.userInn
)
SELECT 
    m.userInn,
    ROUND(m.morning_avg_profit, 0) as morning_profit,
    ROUND(e.evening_avg_profit, 0) as evening_profit
FROM morning_profits m
INNER JOIN evening_profits e ON m.userInn = e.userInn
WHERE m.morning_avg_profit > e.evening_avg_profit
ORDER BY morning_profit ASC
LIMIT 50;