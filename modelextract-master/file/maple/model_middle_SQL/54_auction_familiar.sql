-- Maple Modeling Middle Data:54_auction_familiar
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
    accountid ,
    stringacterid ,
    SUM(CASE WHEN state in (2,7) THEN num ELSE 0 END ) AS buy_num,
    SUM(CASE WHEN state in (3,8) THEN num ELSE 0 END ) AS sell_num
FROM(
    SELECT
        accountid ,
        stringacterid ,
        state,
        count(*) AS num
    FROM
        gtwpd.maple_middle_auction_familiar_log
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
    GROUP BY accountid ,stringacterid ,state
)AA
GROUP BY accountid ,stringacterid
;