-- Maple Modeling Middle Data:51_cash_income
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    buycharactername as charactername,
    nexonclubid AS serviceaccountid,
    accountid,
    SUM(price) AS total_price,
    COUNT(price) AS total_count,
    SUM(CASE WHEN actionid = 1 THEN price ELSE 0 END ) AS buy_price,
    SUM(CASE WHEN actionid = 1 THEN 1 ELSE 0 END ) AS buy_count,
    SUM(CASE WHEN actionid in (2, 9) THEN price ELSE 0 END ) AS gift_price,
    SUM(CASE WHEN actionid in (2, 9) THEN 1 ELSE 0 END ) AS gift_count
FROM
    (
    SELECT
        buycharactername,
        nexonclubid,
        (CASE WHEN buyaccountid = 0 THEN accountid ELSE buyaccountid END) AS accountid,
        price,
        actionid,
        dt,
        world
    FROM
         maple_extract.gamedb_cashitemlog
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
        AND actionid in (1,2,9)
    )AA
GROUP BY
    buycharactername,
    nexonclubid,
    accountid
;
