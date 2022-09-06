select distinct charactername 
FROM
( 
select charactername
FROM maple_extract.globalaccount_accountblockreason 
WHERE dt between [:Date0] and [:Date1]
UNION ALL 
 
select charactername
FROM maple_extract.globalaccount_accountblockbylbd 
WHERE dt between [:Date0] and [:Date1]
)AA
;