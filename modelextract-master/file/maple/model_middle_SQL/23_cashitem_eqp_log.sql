-- Maple Modeling Middle Data : 23_cashitem_eqp_log
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    AAA.*
FROM
(
    SELECT
        cashitemsn	,
        pos	,
        ruc	,
        cuc	,
        i_str	,
        i_dex	,
        i_int	,
        i_luk	,
        i_maxhp	,
        i_maxmp	,
        i_pad	,
        i_mad	,
        i_pdd	,
        i_mdd	,
        i_acc	,
        i_eva	,
        i_speed	,
        i_craft	,
        i_jump	,
        attribute	,
        itemstate	,
        hsv	,
        accountid	,
        characterid	,
        itemid	,
        number	,
        buycharactername	,
        expireddate	,
        commodityid	,
        paybackrate	,
        discountrate	,
        gameworldid	,
        ownerid	,
        storebank	,
        orderno	,
        productno	,
        refundable	,
        sourceflag

        FROM
			(
			SELECT
				*
			FROM
				maple_all.gamedb_itemlocker
			WHERE dt = '[:Date1]'
				AND world = '[:gw]'

			)AA
        INNER JOIN
			(
			SELECT
				*
			FROM
				maple_all.gamedb_cashitem_eqp
			WHERE dt = '[:Date1]'
				AND world = '[:gw]'
			)BB
        ON
            BB.cashitemsn = AA.sn

)AAA
INNER JOIN
(
    SELECT
        characterid
    FROM
        maple_extract.gamedb_character
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
)BBB
ON AAA.characterid = BBB.characterid
;
