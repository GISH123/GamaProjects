SELECT
    AA.characterid AS characterid,
    AA.accountid AS ,
    BB.characterid IS NULL AS is_main,
    '[:gw]' AS gw,
     '[:Date0]' AS dt
FROM

(
    SELECT
        characterid,
        accountid
    FROM
        gtwpd.maple_middle_monthly_login_info
    WHERE
        dt = [:Date0]
        AND world = '[:gw]'

)AA

LEFT JOIN
(
    SELECT
        characterid,
        accountid
    FROM
        gtwpd.maple_middle_monthly_dojangranking
    WHERE
        dt = [:Date0]
        AND world = '[:gw]'
)BB
ON AA.accountid = BB.accountid
AND BB.accountid IS NOT NULL
;