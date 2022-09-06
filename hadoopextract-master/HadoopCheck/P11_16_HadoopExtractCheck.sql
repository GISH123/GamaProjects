DELETE FROM hadoopextract.datacheck
WHERE 1 = 1
    AND datatime >= '[:SelectDate]'::timestamp
    AND datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
    AND ('Game' = '[:GameName]' OR gamename = '[:GameName]');

DELETE FROM hadoopextract.datacheckDetail
WHERE 1 = 1
    AND datatime >= '[:SelectDate]'::timestamp
    AND datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
    AND ('Game' = '[:GameName]' OR gamename = '[:GameName]');

INSERT INTO hadoopextract.datacheckdetail
WITH BASIC_DATA as (
	SELECT
		AA.gamename
		, AA.dbname
		, AA.world
		, AA.tablename
		, SUM(CASE WHEN datatime = '[:SelectDate]' THEN datasize ELSE 0 END) as datanowday
		, Round(
			SUM(
				CASE
					WHEN 1 = 1
					AND datatime <= '[:SelectDate]'::timestamp - INTERVAL '1 Day'
					AND datatime >= '[:SelectDate]'::timestamp - INTERVAL '6 Day'
				THEN datasize
				ELSE 0
				END
			)/6
		) as data7bmean
		, 0 as dbnowday
		, 0 as db7bmean
	FROM hadoopextract.datasize AA
	WHERE 1 = 1
		AND AA.datatime >= '[:SelectDate]'::timestamp - INTERVAL '6 Day'
		AND AA.datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
        AND ('Game' = '[:GameName]' OR AA.gamename = '[:GameName]')
	group by
		AA.gamename
		, AA.dbname
		, AA.world
		, AA.tablename
)
SELECT
	'[:SelectDate]'::timestamp as datatime
	, AAAA.gamename
	, AAAA.dbname
	, AAAA.world
	, AAAA.tablename
	, 1 as errorcount
	, AAAA.data7bmean - AAAA.datanowday as errorsize
	, 'nowday:'||AAAA.datanowday||',bmean:'||AAAA.data7bmean||',datapar:'||AAAA.datapar as message
	, AAAA.datanowday
FROM (
	SELECT
		AAA.gamename
		, AAA.dbname
	    , AAA.world
		, AAA.tablename
		, SUM(AAA.datanowday) as datanowday
		, SUM(AAA.data7bmean) as data7bmean
		, case when SUM(AAA.data7bmean) = 0 then 0 else SUM(AAA.datanowday) / SUM(AAA.data7bmean) end as datapar
		, SUM(AAA.dbnowday) as dbnowday
		, SUM(AAA.db7bmean) as db7bmean
		,  case when SUM(AAA.db7bmean) = 0 then 0 else SUM(AAA.dbnowday) / SUM(AAA.db7bmean) end  as dbpar
	FROM BASIC_DATA AAA
	WHERE 1 = 1
	GROUP BY
		AAA.gamename
		, AAA.dbname
		, AAA.tablename
	    , AAA.world
) AAAA
WHERE 1 = 1
    AND AAAA.datapar < 0.65
	AND AAAA.datanowday = 0
	AND AAAA.data7bmean != 0 ;

INSERT INTO hadoopextract.datacheck
SELECT
    AA.datatime
    , AA.gamename
    , AA.dbname
    , SUM(AA.errorcount) as errorcount
    , SUM(AA.errorsize) as errorsize
    , 'tableerror:' || count(*) as message
    , 0 as  status
FROM hadoopextract.datacheckdetail AA
WHERE 1 = 1
    AND datatime >= '[:SelectDate]'::timestamp
    AND datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
    AND ('Game' = '[:GameName]' OR AA.gamename = '[:GameName]')
GROUP BY
    AA.datatime
    , AA.gamename
    , AA.dbname ;