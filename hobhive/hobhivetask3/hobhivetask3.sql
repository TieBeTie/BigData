ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE kozhagulovra;

-- Найти наиболее прибыльный день месяца для каждого налогоплательщика
WITH daily_profits AS (
    SELECT 
        content.userInn as userInn,
        DAY(FROM_UNIXTIME(CAST(content.dateTime.ddate AS BIGINT) DIV 1000)) as day,
        SUM(COALESCE(content.totalSum, 0)) as total_profit
    FROM kkt_transactions
    WHERE subtype = 'receipt'
        AND content.userInn IS NOT NULL 
        AND content.dateTime.ddate IS NOT NULL
        AND LENGTH(content.userInn) = 10
        AND content.userInn REGEXP '^[0-9]+$'
    GROUP BY content.userInn, DAY(FROM_UNIXTIME(CAST(content.dateTime.ddate AS BIGINT) DIV 1000))
),
ranked_days AS (
    SELECT 
        userInn,
        day,
        total_profit,
        RANK() OVER (PARTITION BY userInn ORDER BY total_profit DESC) as rn
    FROM daily_profits
)
SELECT 
    userInn,
    day,
    total_profit
FROM ranked_days
WHERE rn = 1
ORDER BY userInn ASC
LIMIT 50;