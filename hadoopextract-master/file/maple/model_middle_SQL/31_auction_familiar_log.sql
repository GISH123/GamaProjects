-- Maple Modeling Middle Data :13_auction_familiar_log
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    AA.auctionid  ,
    AA.familiarid  ,
    AA.grade  ,
    AA.level  ,
    AA.optionlevel  ,
    AA.skillid  ,
    AA.option1  ,
    AA.option2  ,
    AA.option3  ,

    BB.tradedate ,
    BB.accountid ,
    BB.stringacterid , --角色ID
    BB.itemid ,
    BB.state ,
    BB.price ,
    BB.itemcount
FROM
    gtwpd.maple_auctiondb_auction_familiar AA
INNER JOIN
    gtwpd.maple_middle_auctionhistory_log BB
ON
    AA.world = BB.world
    AND AA.auctionid = BB.auctionid
    AND AA.familiarid >0
    AND BB.world = '[:gw]'
    AND BB.dt = '[:Date0]'
    AND BB.state in (2,3,7,8)

-- state 2 購買成功放在保管箱，State 3 販賣成功，未領錢，state 7 購買成功並從保管箱領出，state 8 販賣成功並取出楓幣

;

