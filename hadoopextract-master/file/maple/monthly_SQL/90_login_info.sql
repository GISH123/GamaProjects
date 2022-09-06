INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
   CCC.characterid AS characterid,
   AAA.accountid AS accountid,
   gw_count,
   acc_duration,
   total_duration,
   total_duration / acc_duration AS character_time_ratio,
   online_days,
   last_login_date,
   L_SUN,
   L_MON,
   L_TUE,
   L_WED,
   L_THU,
   L_FRI,
   L_SAT,
   O_SUN,
   O_MON,
   O_TUE,
   O_WED,
   O_THU,
   O_FRI,
   O_SAT
FROM
(
    SELECT
        charactername,
        accountid,
        world,
        SUM(total_duration) AS total_duration,
        SUM(online_days) AS online_days,
        MAX(dt) AS last_login_date,
		-- 一星期各日的平均登入時間
        SUM(CASE WHEN weekday = 0 THEN avg_duration ELSE 0 END ) AS L_SUN,
        SUM(CASE WHEN weekday = 1 THEN avg_duration ELSE 0 END ) AS L_MON,
        SUM(CASE WHEN weekday = 2 THEN avg_duration ELSE 0 END ) AS L_TUE,
        SUM(CASE WHEN weekday = 3 THEN avg_duration ELSE 0 END ) AS L_WED,
        SUM(CASE WHEN weekday = 4 THEN avg_duration ELSE 0 END ) AS L_THU,
        SUM(CASE WHEN weekday = 5 THEN avg_duration ELSE 0 END ) AS L_FRI,
        SUM(CASE WHEN weekday = 6 THEN avg_duration ELSE 0 END ) AS L_SAT,
		-- 一星期各日的平均登入日數
        SUM(CASE WHEN weekday = 0 THEN online_days ELSE 0 END ) AS O_SUN,
        SUM(CASE WHEN weekday = 1 THEN online_days ELSE 0 END ) AS O_MON,
        SUM(CASE WHEN weekday = 2 THEN online_days ELSE 0 END ) AS O_TUE,
        SUM(CASE WHEN weekday = 3 THEN online_days ELSE 0 END ) AS O_WED,
        SUM(CASE WHEN weekday = 4 THEN online_days ELSE 0 END ) AS O_THU,
        SUM(CASE WHEN weekday = 5 THEN online_days ELSE 0 END ) AS O_FRI,
        SUM(CASE WHEN weekday = 6 THEN online_days ELSE 0 END ) AS O_SAT
    FROM
    (
        SELECT
            charactername,
            accountid,
            world,
			-- 判斷星期幾
            (datediff( concat_ws('-',substr(dt,1,4),substr(dt,5,2),substr(dt,7,2)) ,'2000-01-01')-1) %7 AS weekday,
            AVG(duration) AS avg_duration,
            SUM(duration) AS total_duration,
            COUNT(*) AS online_days,
            MAX(dt) AS dt
        FROM
            gtwpd.maple_middle_login_duration_new
        WHERE
            dt BETWEEN '[:Date0]' AND '[:DateN]'
            AND world = '[:gw]'
        GROUP BY
			--星期幾判斷
            (datediff( concat_ws('-',substr(dt,1,4),substr(dt,5,2),substr(dt,7,2)) ,'2000-01-01')-1) %7 ,
            accountid,
            world,
            charactername
    )AA
    GROUP BY
        world,
        accountid,
        charactername
)AAA
INNER JOIN
(
    SELECT
        accountid,
        count(DISTINCT world) AS gw_count,
        SUM(duration) AS acc_duration
    FROM
        gtwpd.maple_middle_login_duration_new
    WHERE
        dt between '[:Date0]' and '[:DateN]'
    GROUP BY accountid
)BBB
ON AAA.accountid = BBB.accountid
INNER JOIN
(
    SELECT
        accountid,
        charactername,
        world,
        MAX(characterid) AS characterid
    FROM
        maple_extract.gamedb_character
    WHERE
        dt between '[:Date0]' and '[:DateN]'
    GROUP BY
        accountid,
        charactername,
        world
)CCC
ON 1=1
    AND AAA.charactername = CCC.charactername
    AND AAA.accountid = CCC.accountid
    AND AAA.world = CCC.world
;