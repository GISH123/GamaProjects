-- Maple Modeling Middle Data :19_auction_cashitem_eqp_log
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    AA.auctionid 	,
    AA.worldid 	,
    AA.cashitemsn 	,
    AA.pos 	,
    AA.ruc 	,
    AA.cuc 	,
    AA.i_str 	,
    AA.i_dex 	,
    AA.i_int 	,
    AA.i_luk 	,
    AA.i_maxhp 	,
    AA.i_maxmp 	,
    AA.i_pad 	,
    AA.i_mad 	,
    AA.i_pdd 	,
    AA.i_mdd 	,
    AA.i_acc 	,
    AA.i_eva 	,
    AA.i_speed 	,
    AA.i_craft 	,
    AA.i_jump 	,
    AA.attribute,
    AA.itemid,
    AA.number,
    AA.expireddate,
    AA.itemstate,
    AA.hsv,

    BB.tradedate ,
    BB.accountid ,
    BB.stringacterid , --角色ID
    BB.state ,
    BB.price
FROM
    gtwpd.maple_auctiondb_cashitem_eqp AA
INNER JOIN
    gtwpd.maple_middle_auctionhistory_log BB
ON
    AA.world = BB.world
    AND AA.auctionid = BB.auctionid
    AND BB.world = '[:gw]'
    AND BB.dt = '[:Date0]'
    AND BB.state in (2,3,7,8)
-- state 2 購買成功放在保管箱，State 3 販賣成功，未領錢，state 7 購買成功並從保管箱領出，state 8 販賣成功並取出楓幣
;

