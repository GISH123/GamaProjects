-- Maple Modeling Middle Data:18_auction_cashitem_pet_log
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    
    AA.auctionid	,
    AA.worldid	,
    AA.cashitemsn	,
    AA.pos	,
    AA.petname	,
    AA.petlevel	,
    AA.tameness	,
    AA.repleteness	,
    AA.deaddate	,
    AA.petattribute	,
    AA.petskill	,
    AA.remainlife	,
    AA.attribute	,
    AA.activestate	,
    AA.autobuffskill	,
    AA.pethue	,
    AA.giantrate	,
    AA.itemid	,
    AA.number	,
    AA.expireddate	,
    AA.optionbuff1	,
    AA.optionbuff2	,
    AA.optionbuff3	,
    AA.grade	,

    BB.tradedate ,
    BB.accountid ,
    BB.stringacterid , --角色ID
    BB.state ,
    BB.price
FROM
    gtwpd.maple_auctiondb_cashitem_pet AA
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

