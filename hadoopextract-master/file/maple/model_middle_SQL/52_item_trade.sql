-- Maple Modeling Middle Data:52_item_trade
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

--選出所有參與買賣的人
SELECT
    characterid,
    SUM(sell_cnt) AS sell_cnt,
    SUM(buy_cnt) AS buy_cnt,
    SUM(pass_on) AS pass_on,
    dt,
    world
--計算將每一物品累計起來購買、販賣、轉手數
FROM
(
    SELECT
        characterid,
        itemid,
        MAX(sell_cnt) AS sell_cnt,
        MAX(buy_cnt) AS buy_cnt,
        (CASE WHEN MAX(sell_cnt) > MAX(buy_cnt) THEN MAX(buy_cnt) ELSE MAX(sell_cnt) END) AS pass_on,
        dt,
        world
		--計算每一項物品同日購買、販賣、轉手數
    FROM
    (
        SELECT
            from_ AS characterid,
            count(*) AS sell_cnt,
            0 AS buy_cnt,
            itemid,
            dt,
            world
        FROM
            gtwpd.maple_middle_itemmovepath_log
        WHERE
            dt = '[:Date0]'
            AND world = '[:gw]'
            AND type_ = 0
            GROUP BY
                from_,
                dt,
                world,
                itemid
        UNION ALL
        SELECT
            to_ AS characterid,
            0 AS sell_cnt,
            count(*) AS buy_cnt,
            itemid,
            dt,
            world
        FROM
            gtwpd.maple_middle_itemmovepath_log
        WHERE
            dt = '[:Date0]'
            AND world = '[:gw]'
            AND type_ = 0
        GROUP BY
            to_,
            dt,
            world,
            itemid
    )AA
    GROUP BY
        dt,
        world,
        characterid,
        itemid
)AAA
GROUP BY
    dt,
    world,
    characterid
;
