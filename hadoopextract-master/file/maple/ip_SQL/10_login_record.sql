-- Maple Middle : Login_Record
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT *
FROM
(
    SELECT
        accountid,
        email,
        charactername,
        type AS login_type,
        LAG(type,1,'ADD') OVER (PARTITION BY charactername ORDER BY logtime DESC) AS logout_type,
        logtime AS login_time,
        LAG(logtime,1,'[:Date1Line]') OVER (PARTITION BY charactername ORDER BY logtime DESC) AS logout_time,
        ip,
        gameworldid,
        dt AS ori_dt
    FROM
    (
        SELECT
            *
        FROM
            gtwpd.maple_iplog
        WHERE 1=1
            AND dt = '[:Date0]'
            AND gw = '[:gw]'
        UNION ALL

        SELECT
            logout_time AS logtime,
            logout_type AS type,
            accountid,
            email,
            charactername,
            ip,
            gameworldid,
            ori_dt AS dt,
            world AS gw
        FROM
            gtwpd.maple_middle_login_record
        WHERE 1=1
            AND dt = '[:Date-1Line]'
            AND world = '[:gw]'
            AND logout_type = 'ADD'
            AND ori_dt >= '[:DateM]'
    )AA
)AAA
WHERE login_type NOT LIKE 'OUT%'
;