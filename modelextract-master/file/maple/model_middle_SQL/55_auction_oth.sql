-- Maple Modeling Middle Data:55_auction_oth
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
    accountid ,
    stringacterid ,
    SUM(CASE WHEN state in (2,7) THEN 1 ELSE 0 END ) AS buy_num,
    SUM(CASE WHEN state in (3,8) THEN 1 ELSE 0 END ) AS sell_num
FROM(
    SELECT
        accountid ,
        stringacterid ,
        state
    FROM
        gtwpd.maple_middle_auction_con_log
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
    UNION ALL
    SELECT
        accountid ,
        stringacterid ,
        state
    FROM
        gtwpd.maple_middle_auction_ins_log
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
        UNION ALL
    SELECT
        accountid ,
        stringacterid ,
        state
    FROM
        gtwpd.maple_middle_auction_etc_log
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
)AA
GROUP BY accountid ,stringacterid
;