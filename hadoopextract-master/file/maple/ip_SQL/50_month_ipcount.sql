SELECT
	month,
	ip_count,
	count(*) AS acc_count
FROM
(
	SELECT
		accountid,
		SUBSTR(dt,1,6) AS month,
            CAST(PERCENTILE(ip_count,0.5) AS INT) AS ip_count
	FROM
	(
		SELECT
			*
		FROM
			gtwpd.maple_middle_login_ipcount
		WHERE
		    rank = 1

	)AA
	GROUP BY
		accountid,
		SUBSTR(dt,1,6)
)AAA
GROUP BY 
	month,
	ip_count;