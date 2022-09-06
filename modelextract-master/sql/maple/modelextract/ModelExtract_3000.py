import time

class ModelExtract_3000() :

    @classmethod
    def insert3001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 3001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='3001')
            SELECT
                AA.CommonData_1 as CommonData_1
                , AA.CommonData_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , SUM(1) as UniqueInt_1 --裝備總數量
                , SUM(CASE WHEN UniqueStr_1 = 'eqp' THEN 1 ELSE 0 END) as UniqueInt_2 --裝備類別數量一
                , SUM(CASE WHEN UniqueStr_1 = 'opt' THEN 1 ELSE 0 END) as UniqueInt_3 --裝備類別數量二
                , SUM(CASE WHEN UniqueStr_1 = 'casheqp' THEN 1 ELSE 0 END) as UniqueInt_4 --裝備類別數量三
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , null as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , null as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
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
                , null as uniquedbl_1 
                , null as uniquedbl_2 
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
                , null as UniqueTime_1
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
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game='[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = 3011
            GROUP BY
                AA.CommonData_1
                , AA.CommonData_2
                , AA.CommonData_3
                , AA.CommonData_4 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert3002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 3002
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='3002')
            SELECT
                AA.CommonData_1 as CommonData_1
                , AA.CommonData_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , SUM(1) as UniqueInt_1 --裝備總數量
                , SUM(CASE WHEN UniqueStr_1 = 'eqp' THEN 1 ELSE 0 END) as UniqueInt_2 --裝備類別數量一
                , SUM(CASE WHEN UniqueStr_1 = 'opt' THEN 1 ELSE 0 END) as UniqueInt_3 --裝備類別數量二
                , SUM(CASE WHEN UniqueStr_1 = 'casheqp' THEN 1 ELSE 0 END) as UniqueInt_4 --裝備類別數量三
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , null as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , null as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
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
                , null as uniquedbl_1 
                , null as uniquedbl_2 
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
                , null as UniqueTime_1
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
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game='[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = 3012
            GROUP BY
                AA.CommonData_1
                , AA.CommonData_2
                , AA.CommonData_3
                , AA.CommonData_4 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert3011DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 3011
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='3011')
            SELECT
                AAA.serviceid as CommonData_1 --服務ID
                , AAA.accountid as CommonData_2 --帳號ID
                , AAA.characterid as CommonData_3 --角色ID
                , AAA.charactername as CommonData_4 --角色名稱
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AAA.itemid as CommonData_8 --道具ID
                , AAA.itemid as CommonData_9 --道具名稱
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , null as UniqueInt_2
                , null as UniqueInt_3
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , null as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , AAA.itemslottype as UniqueStr_1 --裝備類別一
                , null as UniqueStr_2 --裝備類別二
                , AAA.is_cash as UniqueStr_3 --裝備類別三(是否為現金道具)
                , AAA.pos as UniqueStr_4 --裝備欄位一
                , null as UniqueStr_5 --裝備欄位二
                , null as UniqueStr_6 --裝備欄位三
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
                , null as uniquedbl_1 
                , null as uniquedbl_2 
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
                , null as UniqueTime_1
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
            FROM (
                SELECT
                    AA.CommonData_1 as serviceid
                    , AA.CommonData_2 as accountid
                    , AA.CommonData_3 as characterid
                    , AA.CommonData_4 as charactername
                    , BB.itemid
                    , BB.pos
                    , 0 AS is_cash
                    , 'eqp' AS itemslottype
                FROM gtwpd.modelextract_modelextract AA
                INNER JOIN maple_extract.gamedb_itemslot_eqp BB ON 1 = 1
                    AND AA.commondata_3= BB.characterid
                    AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND BB.world = '[:World]'
                WHERE 1 = 1
                    AND AA.game='[:GameName]'
                    AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND AA.world = '[:World]'
                    AND AA.tablenumber = 11003
                UNION ALL
                SELECT
                    AA.CommonData_1 as serviceid
                    , AA.CommonData_2 as accountid
                    , AA.CommonData_3 as characterid
                    , AA.CommonData_4 as charactername
                    , BB.itemid
                    , BB.pos
                    , 0 AS is_cash
                    , 'opt' AS itemslottype
                FROM gtwpd.modelextract_modelextract AA
                INNER JOIN maple_extract.gamedb_itemslot_opt BB ON 1 = 1
                    AND AA.commondata_3= BB.characterid
                    AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND BB.world = '[:World]'
                WHERE 1 = 1
                    AND AA.game='[:GameName]'
                    AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND AA.world = '[:World]'
                    AND AA.tablenumber = 11003
                UNION ALL
                SELECT
                    AA.CommonData_1 as serviceid
                    , AA.CommonData_2 as accountid
                    , AA.CommonData_3 as characterid
                    , AA.CommonData_4 as charactername
                    , BB.itemid
                    , CC.pos
                    , 1 AS is_cash
                    , 'casheqp' AS itemslottype
                FROM gtwpd.modelextract_modelextract AA
                INNER JOIN maple_extract.gamedb_itemlocker BB ON 1 = 1
                    AND AA.commondata_3 = BB.characterid
                    AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND BB.world = '[:World]'
                INNER JOIN maple_extract.gamedb_cashitem_eqp CC ON 1 = 1
                    AND BB.sn = CC.cashitemsn
                    AND CC.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND CC.world = '[:World]'
                WHERE 1 = 1
                    AND AA.game='[:GameName]'
                    AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND AA.world = '[:World]'
                    AND AA.tablenumber = 11003
            ) AAA ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert3012DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 3012
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='3012')
            SELECT
                AAA.serviceid as CommonData_1 --服務ID
                , AAA.accountid as CommonData_2 --帳號ID
                , AAA.characterid as CommonData_3 --角色ID
                , AAA.charactername as CommonData_4 --角色名稱
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AAA.itemid as CommonData_8 --道具ID
                , AAA.itemid as CommonData_9 --道具名稱
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , null as UniqueInt_2
                , null as UniqueInt_3
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , null as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , AAA.itemslottype as UniqueStr_1 --裝備類別一
                , null as UniqueStr_2 --裝備類別二
                , AAA.is_cash as UniqueStr_3 --裝備類別三
                , AAA.pos as UniqueStr_4 --裝備欄位一
                , null as UniqueStr_5 --裝備欄位二
                , null as UniqueStr_6 --裝備欄位三
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
                , null as uniquedbl_1 
                , null as uniquedbl_2 
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
                , null as UniqueTime_1
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
            FROM (
                SELECT
                    AA.CommonData_1 as serviceid
                    , AA.CommonData_2 as accountid
                    , AA.CommonData_3 as characterid
                    , AA.CommonData_4 as charactername
                    , BB.itemid
                    , BB.pos
                    , 0 AS is_cash
                    , 'eqp' AS itemslottype
                FROM gtwpd.modelextract_modelextract AA
                INNER JOIN maple_extract.gamedb_itemslot_eqp BB ON 1 = 1
                    AND AA.CommonData_3= BB.characterid
                    AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND BB.world = '[:World]'
                WHERE 1 = 1
                    AND AA.game='[:GameName]'
                    AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND AA.world = '[:World]'
                    AND AA.tablenumber = 11003
                UNION ALL
                SELECT
                    AA.CommonData_1 as serviceid
                    , AA.CommonData_2 as accountid
                    , AA.CommonData_3 as characterid
                    , AA.CommonData_4 as charactername
                    , BB.itemid
                    , BB.pos
                    , 0 AS is_cash
                    , 'opt' AS itemslottype
                FROM gtwpd.modelextract_modelextract AA
                INNER JOIN maple_extract.gamedb_itemslot_opt BB ON 1 = 1
                    AND AA.CommonData_3= BB.characterid
                    AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND BB.world = '[:World]'
                WHERE 1 = 1
                    AND AA.game='[:GameName]'
                    AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND AA.world = '[:World]'
                    AND AA.tablenumber = 11003
                UNION ALL
                SELECT
                    AA.CommonData_1 as serviceid
                    , AA.CommonData_2 as accountid
                    , AA.CommonData_3 as characterid
                    , AA.CommonData_4 as charactername
                    , BB.itemid
                    , CC.pos
                    , 1 AS is_cash
                    , 'casheqp' AS itemslottype
                FROM gtwpd.modelextract_modelextract AA
                INNER JOIN maple_extract.gamedb_itemlocker BB ON 1 = 1
                    AND AA.CommonData_3 = BB.characterid
                    AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                    AND BB.world = '[:World]'
                INNER JOIN maple_extract.gamedb_cashitem_eqp CC ON 1 = 1
                    AND BB.sn = CC.cashitemsn
                    AND CC.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                    AND CC.world = '[:World]'
                WHERE 1 = 1
                    AND AA.game='[:GameName]'
                    AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND AA.world = '[:World]'
                    AND AA.tablenumber = 11003
            ) AAA ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

