-- Maple Middle : Login_Duration_new
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
	charactername,
	accountid,
	SUM( unix_timestamp(logout_time) - unix_timestamp(login_time) ) AS duration,
	email,
	min(ori_dt) AS login_dt

FROM gtwpd.maple_middle_login_record
WHERE dt = '[:Date0]' and world ='[:gw]'
GROUP BY accountid, email, charactername
;