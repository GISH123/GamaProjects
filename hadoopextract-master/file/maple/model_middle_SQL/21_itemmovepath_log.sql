-- Maple Modeling Middle Data : 11_itemmovepath_log
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
	date_time ,
	gameworldid	,
	itemsn ,
	itemid ,
	type ,
	from_ ,
	to_ ,
	through	,
	fieldid
FROM
    gtwpd.maple_claimdb_itemmovepath
WHERE
    date_time between '[:Date-0]' and '[:Date-1]'
    AND gameworldid = '[:gwid]'
;
