ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE kozhagulovra;

-- Поиск нарушителей закона 54-ФЗ: транзакции receipt между closeShift и openShift
WITH all_transactions AS (
    SELECT 
        content.userInn as userInn,
        kktRegId,
        subtype,
        FROM_UNIXTIME(CAST(content.dateTime.ddate AS BIGINT) DIV 1000) as transaction_time
    FROM kkt_transactions
    WHERE content.userInn IS NOT NULL 
        AND content.dateTime.ddate IS NOT NULL
        AND kktRegId IS NOT NULL
        AND subtype IN ("receipt", "openShift", "closeShift")
),
ordered_transactions AS (
    SELECT 
        userInn,
        kktRegId,
        subtype,
        transaction_time,
        LAG(subtype) OVER (PARTITION BY userInn, kktRegId ORDER BY transaction_time) as prev_subtype,
        LEAD(subtype) OVER (PARTITION BY userInn, kktRegId ORDER BY transaction_time) as next_subtype,
        ROW_NUMBER() OVER (PARTITION BY userInn, kktRegId ORDER BY transaction_time) as rn
    FROM all_transactions
),
violations AS (
    SELECT DISTINCT userInn
    FROM ordered_transactions
    WHERE (
        -- Транзакция receipt после closeShift и до openShift
        (subtype = "receipt" AND prev_subtype = "closeShift" AND next_subtype = "openShift")
        OR
        -- Транзакция receipt после closeShift без последующего openShift
        (subtype = "receipt" AND prev_subtype = "closeShift" AND next_subtype IS NULL)
        OR
        -- Транзакция receipt до первого openShift
        (subtype = "receipt" AND prev_subtype IS NULL AND rn = 1)
        OR
        -- Транзакция receipt после последнего closeShift
        (subtype = "receipt" AND prev_subtype = "closeShift" AND next_subtype != "openShift")
    )
)
SELECT userInn
FROM violations
ORDER BY userInn ASC
LIMIT 50;