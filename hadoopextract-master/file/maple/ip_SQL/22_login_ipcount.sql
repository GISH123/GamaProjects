-- Maple Middle : Login_IP_COUNT
-- 以有登入角色，看同登IP數
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    accountid,
    ip,
    same_ip_acc_num,                   --同ip該帳號登入次數
    logout_time AS last_logout_time,   --最後登出時間
    dense_rank() over(PARTITION BY accountid ORDER BY same_ip_acc_num DESC, logout_time DESC) as rank,  --最常登入的ip排名
    sum(1) over(PARTITION BY ip) as ip_count --該IP登入帳號數
    FROM(
        SELECT
            accountid,
            split(ip,':')[0] as ip ,
            count(*) as same_ip_acc_num,
            max(logout_time) AS logout_time
        FROM
            gtwpd.maple_middle_login_record
        WHERE
            dt = '[:Date0]'
        GROUP BY
            accountid ,
            split(ip,':')[0]
    )AA
;