ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE kozhagulovra;

-- Создание таблицы в формате TEXT
DROP TABLE IF EXISTS kkt_text_format;
CREATE TABLE kkt_text_format 
STORED AS TEXTFILE
AS SELECT 
    content.userInn as userInn, 
    content.totalSum as totalSum 
FROM kkt_transactions
WHERE subtype = "receipt" AND content.userInn IS NOT NULL AND content.totalSum IS NOT NULL;

-- Создание таблицы в формате ORC
DROP TABLE IF EXISTS kkt_orc_format;
CREATE TABLE kkt_orc_format 
STORED AS ORC
AS SELECT 
    content.userInn as userInn, 
    content.totalSum as totalSum 
FROM kkt_transactions
WHERE subtype = "receipt" AND content.userInn IS NOT NULL AND content.totalSum IS NOT NULL;

-- Создание таблицы в формате PARQUET
DROP TABLE IF EXISTS kkt_parquet_format;
CREATE TABLE kkt_parquet_format 
STORED AS PARQUET
AS SELECT 
    content.userInn as userInn, 
    content.totalSum as totalSum 
FROM kkt_transactions
WHERE subtype = "receipt" AND content.userInn IS NOT NULL AND content.totalSum IS NOT NULL;

-- Тестовый запрос для определения налогоплательщика с наибольшей прибылью
-- Выполняется на ORC таблице (оптимальный формат для аналитики)
SELECT 
    userInn,
    SUM(totalSum) as total_profit
FROM kkt_orc_format
GROUP BY userInn
ORDER BY total_profit DESC
LIMIT 1;