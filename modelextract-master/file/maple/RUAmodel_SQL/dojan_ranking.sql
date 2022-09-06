SELECT
    characterid,
    accountid,
    count(*) AS type,
    '[:gw]' AS gw,
     '[:Date0]' AS dt
FROM
(
    SELECT
        AA.characterid AS characterid,
        AA.accountid AS accountid
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

        INNER JOIN
        (
            SELECT
                DISTINCT accountid
            FROM
                gtwpd.maple_middle_monthly_dojangranking
            WHERE
                dt = [:Date0]
                AND world = '[:gw]'
        )BB
        ON AA.accountid = BB.accountid

        UNION ALL

        SELECT
            characterid,
            accountid
        FROM
            gtwpd.maple_middle_monthly_dojangranking
        WHERE
            dt = [:Date0]
            AND world = '[:gw]'
)AAA
GROUP BY characterid, accountid
;