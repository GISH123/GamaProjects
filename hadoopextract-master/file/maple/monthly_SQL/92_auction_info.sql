INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
	accountid ,
    characterid ,
    SUM(CASE WHEN table_name  = 'eqp' THEN buy_num ELSE 0 END)  AS auc_eqp_buy,
    SUM(CASE WHEN table_name  = 'eqp' THEN sell_num ELSE 0 END ) AS auc_eqp_sell,
    SUM(CASE WHEN table_name  = 'fam' THEN buy_num ELSE 0 END)  AS auc_fam_buy,
    SUM(CASE WHEN table_name  = 'fam' THEN sell_num ELSE 0 END ) AS auc_fam_sell,
    SUM(CASE WHEN table_name  = 'oth' THEN buy_num ELSE 0 END)  AS auc_oth_buy,
    SUM(CASE WHEN table_name  = 'oth' THEN sell_num ELSE 0 END ) AS auc_oth_sell,
    SUM(CASE WHEN table_name  = 'cash' THEN buy_num ELSE 0 END)  AS auc_cash_buy,
    SUM(CASE WHEN table_name  = 'cash' THEN sell_num ELSE 0 END ) AS auc_cash_sell
FROM
(
	SELECT
	    accountid ,
        characterid ,
        buy_num,
        sell_num,
        'eqp' AS table_name
	FROM
		gtwpd.maple_middle_auction_eqp
	WHERE
	    world = '[:gw]'
		AND dt between '[:Date0]' AND '[:DateN]'

    UNION ALL

	SELECT
	    accountid ,
        characterid ,
        buy_num,
        sell_num,
        'fam' AS table_name
	FROM
		gtwpd.maple_middle_auction_familiar
	WHERE
	    world = '[:gw]'
		AND dt between '[:Date0]' AND '[:DateN]'

    UNION ALL

	SELECT
	    accountid ,
        characterid ,
        buy_num,
        sell_num,
        'oth' AS table_name
	FROM
		gtwpd.maple_middle_auction_oth
	WHERE
	    world = '[:gw]'
		AND dt between '[:Date0]' AND '[:DateN]'

    UNION ALL
	SELECT
	    accountid ,
        characterid ,
        buy_num,
        sell_num,
        'cash' AS table_name
	FROM
		gtwpd.maple_middle_auction_cash
	WHERE
	    world = '[:gw]'
		AND dt between '[:Date0]' AND '[:DateN]'

)BB

GROUP BY accountid ,characterid
;