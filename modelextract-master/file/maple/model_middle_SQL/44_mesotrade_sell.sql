-- Maple Modeling Middle Data:44_mesotrade_sell
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
	characterid, 
	COUNT(*) AS sell_request,
	SUM(number) AS sell_volumn,
	ROUND(sum(quantity)/sum(number),4) AS sell_avg_price
FROM
    (SELECT characterid,
        number,
        Price*Number AS quantity,
        dt,
        world
    FROM  maple_extract.gamedb_mesoexchangelog
    WHERE
        logtype = 4
        AND dt = '[:Date0]'
        AND world = '[:gw]'
    ) AS AA
GROUP BY characterid;
