SELECT real_types, COUNT(*) FROM
(
	SELECT accountid, string_agg(DISTINCT BB.type,',') AS real_types FROM
		result_rua.rua[:ver]_group_[:dt] AA
	INNER JOIN
		otherdata.account_type BB
	ON
		AA."cluster"  = BB."cluster" 
	GROUP BY  accountid 
)BB
group by real_types;