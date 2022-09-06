-- Maple Modeling Middle Data : 43_mesotrade_buy
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    AA.auctionid	,
    AA.worldid	,
    AA.itemid_eqp	,
    AA.ruc	,
    AA.cuc	,
    AA.i_str	,
    AA.i_dex	,
    AA.i_int	,
    AA.i_luk	,
    AA.i_maxhp	,
    AA.i_maxmp	,
    AA.i_pad	,
    AA.i_mad	,
    AA.i_pdd	,
    AA.i_mdd	,
    AA.i_acc	,
    AA.i_eva	,
    AA.i_speed	,
    AA.i_craft	,
    AA.i_jump	,
    AA.expiredate	,
    AA.title	,
    AA.attribute	,
    AA.itemsn,
    AA.leveluptype	,
    AA.level	,
    AA.exp,
    AA.durability	,
    AA.iuc	,
    AA.i_pvpdamage	,
    AA.i_reducereq	,
    AA.specialattribute	,
    AA.durabilitymax	,
    AA.i_increq	,
    AA.growthenchant	,
    AA.psenchant	,
    AA.bdr	,
    AA.imdr	,
    AA.damr	,
    AA.statr	,
    AA.cuttable	,
    AA.exgradeoption,
    AA.itemstate,

    --	原本OPT
    CC.grade	,
    CC.chuc		,
    CC.option1		,
    CC.option2		,
    CC.option3		,
    CC.socket1		,
    CC.socket2		,
    CC.option6		,
    CC.option7		,
    CC.soulskill	,
    CC.soulsocketid		,
    CC.souloption		,
    CC.arc_count		,
    CC.i_arc	,
    CC.arc_level,
    CC.hsv	,

    -- 拍賣基本資料
    BB.tradedate ,
    BB.accountid  ,
    BB.stringacterid , --角色ID
    BB.itemid  ,
    BB.state  ,
    BB.price ,
    BB.itemcount 
FROM
    gtwpd.maple_auctiondb_auction_eqp AA
INNER JOIN
    gtwpd.maple_middle_auctionhistory_log BB
INNER JOIN
    gtwpd.maple_auctiondb_auction_opt CC

ON
    AA.world = BB.world
    AND AA.auctionid = BB.auctionid
    AND BB.world = '[:gw]'
    AND BB.dt = '[:Date0]'
    AND BB.state in (2,3,7,8)
    AND AA.world = CC.world
    AND AA.auctionid = CC.auctionid
-- state 2 購買成功放在保管箱，State 3 販賣成功，未領錢，state 7 購買成功並從保管箱領出，state 8 販賣成功並取出楓幣
-- 先不篩選資料內的萌寵資料，於建模用之中繼站資料再篩選。

;

