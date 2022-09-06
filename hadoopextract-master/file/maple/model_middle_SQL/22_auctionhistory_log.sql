-- Maple Modeling Middle Data: 12_auctionhistory_log
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    AA.sn,
    AA.auctionid,
    AA.accountid,
    AA.stringacterid,
    AA.itemid,
    AA.state,
    AA.price,
    AA.date,
    AA.initprice,
    AA.itemcount,
    AA.worldid,

    BB.auctiontype int ,
    BB.itemtype int ,
    BB.stringname string ,
    BB.secondprice bigint ,
    BB.directprice bigint ,
    BB.enddate timestamp ,
    BB.biduserid int ,
    BB.bidusername string ,
    BB.nexonoid bigint ,
    BB.registerdate timestamp ,
    BB.bidworld int ,
    BB.tableid int ,
    BB.tradedate timestamp
FROM
    gtwpd.maple_auctiondb_auctionhistory AA
INNER JOIN
    gtwpd.maple_auctiondb_auctionitem BB
ON
    AA.auctionid = BB.auctionid
    AND AA.world = BB.world
    AND AA.world = '[:gw]'
    AND tradedate between '[:Date-0]' and '[:Date-1]'

;
