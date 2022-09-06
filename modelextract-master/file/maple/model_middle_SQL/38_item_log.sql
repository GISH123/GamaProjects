-- Maple Modeling Middle Data: 25_item_log
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    AAA.*
FROM
(
    SELECT
        characterid,
        itemid ,
        pos,
        0 AS is_cash
    FROM
		-- EQP
         maple_extract.gamedb_itemslot_eqp
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'

    UNION ALL

    SELECT
        characterid,
        itemid ,
        pos,
        0 AS is_cash
    FROM
        --OPT
          maple_extract.gamedb_itemslot_opt
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'

    UNION ALL

    SELECT
        characterid,
        itemid ,
        pos ,
        1 AS is_cash
    FROM
         gtwpd.maple_middle_cashitem_eqp_log
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
)AAA

-- 篩選該日有上線的
;