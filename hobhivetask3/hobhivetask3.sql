ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE kozhagulovra;

-- Найти наиболее прибыльный день месяца для каждого налогоплательщика
WITH daily_profits AS (
    SELECT 
        content.userInn as userInn,
        DAY(FROM_UNIXTIME(CAST(content.dateTime.ddate AS BIGINT) DIV 1000)) as day_of_month,
        COALESCE(content.totalSum, 0) as totalSum
    FROM kkt_transactions
    WHERE subtype = "receipt" 
        AND content.userInn IS NOT NULL 
        AND content.dateTime.ddate IS NOT NULL
),
daily_aggregated AS (
    SELECT 
        userInn,
        day_of_month,
        SUM(totalSum) as daily_profit
    FROM daily_profits
    GROUP BY userInn, day_of_month
),
ranked_days AS (
    SELECT 
        userInn,
        day_of_month,
        daily_profit,
        ROW_NUMBER() OVER (PARTITION BY userInn ORDER BY daily_profit DESC, day_of_month ASC) as rank_num
    FROM daily_aggregated
)
SELECT 
    userInn,
    day_of_month,
    daily_profit
FROM ranked_days
WHERE rank_num = 1
ORDER BY userInn ASC; 