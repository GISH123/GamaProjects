-- Maple Modeling Middle Data : 14_auction_con_log
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    AA.auctionid ,
    AA.worldid ,
    AA.itemid_con ,
    AA.number ,
    AA.expiredate ,
    AA.title ,
    AA.attribute ,
    AA.itemsn ,

    BB.tradedate ,
    BB.accountid ,
    BB.stringacterid , --角色ID
    BB.itemid ,
    BB.state ,
    BB.price ,
    BB.itemcount
FROM
    gtwpd.maple_auctiondb_auction_con AA
INNER JOIN
    gtwpd.maple_middle_auctionhistory_log BB
ON
    AA.world = BB.world
    AND AA.auctionid = BB.auctionid
    AND BB.world = '[:gw]'
    AND BB.dt = '[:Date0]'
    AND BB.state in (2,3,7,8)
-- state 2 購買成功放在保管箱，State 3 販賣成功，未領錢，state 7 購買成功並從保管箱領出，state 8 販賣成功並取出楓幣
-- 先不篩選資料內的萌寵資料，於建模用之中繼站資料再篩選。

;

