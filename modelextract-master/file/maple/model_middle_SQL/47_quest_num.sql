-- Maple Modeling Middle Data:47_quest_num
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
characterid
, sum(case when questtype = 'A' then questcount else 0 end ) as Q_daily
, sum(case when questtype = 'B' then questcount else 0 end ) as Q_milestone
, sum(case when questtype = 'M' then questcount else 0 end ) as Q_mday
, sum(case when questtype = 'H' then questcount else 0 end ) as Q_happyday
, sum(case when questtype = 'W' then questcount else 0 end ) as Q_wulin
, sum(case when questtype = 'X' then questcount else 0 end ) as Q_battlefield
, sum(case when questtype is null then questcount else 0 end ) as Q_other
FROM (
	SELECT
	    characterid,
	    questtype,
	    count(characterid) as questcount
	FROM(
	    SELECT
	        characterid,
	        qrkey
	    FROM
	         maple_extract.gamedb_questcompletenew
	    WHERE
	        dt = '[:Date0]'
	        AND world = '[:gw]'
	    )AA
	LEFT JOIN gtwpd.maple_othertable_questtype AS BB
	ON AA.qrkey = BB.questcode
	GROUP BY characterid, questtype
) A
group by characterid;
