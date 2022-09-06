-- Maple Modeling Middle Data:43_mesotrade_buy
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
	characterid, 
	COUNT(*) AS buy_request,
	SUM(number) AS buy_volumn,
	ROUND(sum(quantity)/sum(number),4) AS buy_avg_price
FROM
    (SELECT characterid,
        number,
        Price*Number AS quantity,
        dt,
        world
    FROM  maple_extract.gamedb_mesoexchangelog

    WHERE
        logtype = 3
        AND dt = '[:Date0]'
        AND world = '[:gw]'
    ) AS AA
GROUP BY characterid;
