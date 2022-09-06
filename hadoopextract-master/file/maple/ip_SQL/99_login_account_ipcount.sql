-- Maple Middle : Login_account_IP_COUNT
-- 以有登入遊戲者(可能未登角色)，看同登IP數
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    accountid,
    ip,
    same_ip_acc_num,      --同IP、帳號 登入次數
    login_time AS last_logout_time,   --最後登出時間
    dense_rank() over(PARTITION BY accountid ORDER BY same_ip_acc_num DESC, login_time DESC) as rank,  --最常登入的ip排名
    sum(1) over(PARTITION BY ip) as ip_count --同IP帳號數
    FROM(
        SELECT
            accountid,
            split(ip,':')[0] as ip ,
            count(*) as same_ip_acc_num,
            max(logtime) AS login_time
        FROM
            gtwpd.maple_middle_login_account
        WHERE
            dt = '[:Date0]'
        GROUP BY
            accountid ,
            split(ip,':')[0]
    )AA
;