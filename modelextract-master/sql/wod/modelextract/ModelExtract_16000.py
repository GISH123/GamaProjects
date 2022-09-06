import datetime


class ModelExtract_16000 :

    @classmethod
    def insert16001DataSQL(self, makeInfo):
        makeTime = makeInfo["makeTime"]
        orderInsertSQLCode1_Init = """
            -- Make [:GameName] ModelData 16001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='16001')
            SELECT 
                AA.userseq as CommonData_1
                , AA.userseq as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , AA.platformuserid as CommonData_6
                , null as CommonData_7
                , AA.productid as CommonData_8
                , AA.TYPE as CommonData_9
                , CASE 
                    WHEN AA.platformtype = 4 THEN 'GUEST' 
                    WHEN AA.platformtype = 5 THEN 'FACEBOOK' 
                    WHEN AA.platformtype = 6 THEN 'APPLE'
                    WHEN AA.platformtype = 7 THEN 'BEANFUN'
                    ELSE 'OTHER' 
                  END  as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , AA.realcash as UniqueInt_1
                , 1 as UniqueInt_2
                , AA.realcash as UniqueInt_3
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , AA.market as UniqueInt_11
                , AA.state as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , AA.currencycode as UniqueStr_1
                , CASE 
                    WHEN AA.market = 1 THEN 'GOOGLE' 
                    WHEN AA.market = 2 THEN 'APPLE' 
                    ELSE 'OTHER' 
                  END as UniqueStr_2
                , CASE 
                    WHEN AA.state = 1 THEN 'NOFINISH1' 
                    WHEN AA.state = 2 THEN 'NOFINISH2' 
                    WHEN AA.state = 3 THEN 'NOFINISH3' 
                    WHEN AA.state = 4 THEN 'FINISH' 
                    ELSE 'OTHER' 
                  end as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , null as uniquestr_11 
                , null as uniquestr_12 
                , null as uniquestr_13 
                , null as uniquestr_14 
                , null as uniquestr_15 
                , null as uniquestr_16 
                , null as uniquestr_17 
                , null as uniquestr_18 
                , null as uniquestr_19 
                , null as uniquestr_20 
                , AA.realcash as uniquedbl_1 
                , AA.realcash as uniquedbl_2 
                , null as uniquedbl_3 
                , null as uniquedbl_4 
                , null as uniquedbl_5 
                , null as uniquedbl_6 
                , null as uniquedbl_7 
                , null as uniquedbl_8 
                , null as uniquedbl_9 
                , null as uniquedbl_10 
                , null as uniquedbl_11 
                , null as uniquedbl_12 
                , null as uniquedbl_13 
                , null as uniquedbl_14 
                , null as uniquedbl_15 
                , null as uniquedbl_16 
                , null as uniquedbl_17 
                , null as uniquedbl_18 
                , null as uniquedbl_19 
                , null as uniquedbl_20
                , AA.successdate_tw as UniqueTime_1
                , null as UniqueTime_2
                , null as UniqueTime_3
                , null as otherstr_1 
                , null as otherstr_2 
                , null as otherstr_3 
                , null as otherstr_4 
                , null as otherstr_5 
                , null as otherstr_6 
                , null as otherstr_7 
                , null as otherstr_8 
                , null as otherstr_9 
                , null as otherstr_10
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM wod_extract.account_t_cash_ntd AA
            WHERE 1 = 1 
                AND AA.dt = '[:DataDT]' 
                AND ( 1 != 1 
                    OR AA.serverseq = '[:World]' 
                    OR ( AA.serverseq = '111' AND '1011' = '[:World]' )  -- 1011的世界編號錯誤
                    OR '[:DateLine]' >= '2021-04-27'                      -- 2021-04-27以後的LOG編號錯誤
                ) 
                AND AA.successdate_tw >= DATE_ADD('[:DateLine]',0)
                AND AA.successdate_tw < DATE_ADD('[:DateLine]',1) ;
        """
        orderInsertSQLCode1 = ""
        print(makeTime)
        if makeTime == datetime.datetime(2021, 3, 22, 0, 0, 0, 0) :
            orderInsertSQLCode1 = orderInsertSQLCode1_Init.replace('[:DataDT]', '20210323')
        elif makeTime == datetime.datetime(2021, 3, 25, 0, 0, 0, 0) :
            orderInsertSQLCode1 = orderInsertSQLCode1_Init.replace('[:DataDT]', '20210326')
        elif makeTime <= datetime.datetime(2021, 2, 28, 0, 0, 0, 0) :
            orderInsertSQLCode1 = orderInsertSQLCode1_Init.replace( '[:DataDT]','20210301')
        elif makeTime >= datetime.datetime(2021, 3, 1, 0, 0, 0, 0) :
            orderInsertSQLCode1 = orderInsertSQLCode1_Init.replace( '[:DataDT]','[:DateNoLine]')
        return "OrderInsert" , [orderInsertSQLCode1]

    @classmethod
    def insert16002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelData 16001 遊戲商城消費細項 遊戲商城購買紀錄有正負號請注意
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='16002')
            SELECT 
                BBB.accountno -- 帳號名稱
                , BBB.accountno -- 帳號名稱
                , BBB.charid -- 角色ID
                , BBB.charname -- 角色名稱
                , null as CommonData_5
                , BBB.accountname as CommonData_6 -- 平台openID
                , null as CommonData_7
                , BBB.itemid as CommonData_8 -- 商品ID
                , BBB.itemid as CommonData_9 -- 道具名稱 假如無道具名稱則用ID顯示
                , BBB.accounttype as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , get_json_object(BBB.assetchangearray ,'$.assetchangecnt') as UniqueInt_1 --道具總價
                , 1 as UniqueInt_2 -- 道具數量
                , get_json_object(BBB.assetchangearray ,'$.assetchangecnt') as UniqueInt_3 --道具單價
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , get_json_object(BBB.assetchangearray ,'$.assetcategory') as UniqueInt_11 -- 貨幣類型ID
                , get_json_object(BBB.assetchangearray ,'$.assetflowtype') as UniqueInt_12 -- 得到或消耗 1=得到 2=消耗
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , get_json_object(BBB.assetchangearray ,'$.assettype') as UniqueStr_1 -- 貨幣類型
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , BBB.type as uniquestr_11 -- 資料類別　
                , BBB.shopid as uniquestr_12 -- 商城ID
                , null as uniquestr_13 
                , null as uniquestr_14 
                , null as uniquestr_15 
                , null as uniquestr_16 
                , null as uniquestr_17 
                , null as uniquestr_18 
                , null as uniquestr_19 
                , null as uniquestr_20 
                , get_json_object(BBB.assetchangearray ,'$.assetchangecnt') as uniquedbl_1 --貨幣消耗
                , get_json_object(BBB.assetchangearray ,'$.assetchangecnt') as uniquedbl_2 --道具單價
                , null as uniquedbl_3 
                , null as uniquedbl_4 
                , null as uniquedbl_5 
                , null as uniquedbl_6 
                , null as uniquedbl_7 
                , null as uniquedbl_8 
                , null as uniquedbl_9 
                , null as uniquedbl_10 
                , null as uniquedbl_11 
                , null as uniquedbl_12 
                , null as uniquedbl_13 
                , null as uniquedbl_14 
                , null as uniquedbl_15 
                , null as uniquedbl_16 
                , null as uniquedbl_17 
                , null as uniquedbl_18 
                , null as uniquedbl_19 
                , null as uniquedbl_20
                , regexp_replace(BBB.createdate,'T',' ') as UniqueTime_1 -- 資料時間
                , null as UniqueTime_2
                , null as UniqueTime_3
                , null as otherstr_1 
                , null as otherstr_2 
                , null as otherstr_3 
                , null as otherstr_4 
                , null as otherstr_5 
                , null as otherstr_6 
                , null as otherstr_7 
                , null as otherstr_8 
                , null as otherstr_9 
                , null as otherstr_10
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , AAA.DATA as UniqueJson_1 -- 原始資料
            FROM wod_extract.topics_char_ingameshopbuy AAA
            LATERAL VIEW JSON_TUPLE ( 
                AAA.DATA
                , 'type'
                , 'createdate'
                , 'accountno'
                , 'accountname'
                , 'accounttype'
                , 'charid'
                , 'charname'
                , 'shopid'
                , 'itemid'
                , 'assetchangearray'
                , 'worldid'
            ) BBB AS
                type
                , createdate
                , accountno
                , accountname
                , accounttype
                , charid
                , charname
                , shopid
                , itemid
                , assetchangearray
                , worldid
            WHERE 1 = 1 
                AND AAA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd')
                AND AAA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                AND BBB.createdate < DATE_ADD('[:DateLine]',1)
                AND BBB.createdate >= DATE_ADD('[:DateLine]',0)
                AND ( 1 != 1 
                    OR BBB.worldid = '[:World]' 
                    OR ( BBB.worldid = '111' AND '1011' = '[:World]' )  -- 1011的世界編號錯誤
                    OR '[:DateLine]' >= '2021-04-27'                      -- 2021-04-27以後的LOG編號錯誤
                ) ; 
        """
        return "OrderInsert", [orderInsertSQLCode1]

