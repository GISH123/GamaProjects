import time


class ModelExtract_16000 :

    @classmethod
    def insert16009DataSQL(self, makeInfo):
        # 商城相關
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 16009
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]', dt='[:DateNoLine]',world='[:World]',tablenumber='16009')
            SELECT 
                LOWER(AA.nexonclubid) AS CommonData_1
                , BB.accountid   AS CommonData_2
                , BB.characterid AS CommonData_3
                , AA.buycharactername AS CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AA.commodityid as CommonData_8
                , NVL(CC.itemname,AA.commodityid) as CommonData_9
                , AA.itemid as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , 1  as UniqueInt_2
                , null as UniqueInt_3
                , AA.number as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , AA.actionid as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , 'bf point' as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , (CASE 
                    WHEN AA.actionid = 1 THEN '購買'
                    WHEN AA.actionid = 2 THEN '送禮'
                    WHEN AA.actionid = 7 THEN 'RollBack'
                    WHEN AA.actionid = 9 THEN '送禮2'
                END) as UniqueStr_11
                , null as UniqueStr_12
                , null as UniqueStr_13
                , null as UniqueStr_14
                , null as UniqueStr_15
                , null as UniqueStr_16
                , null as UniqueStr_17
                , null as UniqueStr_18
                , null as UniqueStr_19
                , null as UniqueStr_20
                , AA.price as uniquedbl_1 
                , AA.price as uniquedbl_2 
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
                , AA.registerdate as UniqueTime_1
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
                , array(null) as UniqueArray_01
                , array(null) as UniqueArray_02
                , null as UniqueJson_01
            FROM maple_extract.gamedb_cashitemlog AA
            LEFT JOIN maple_all.gamedb_character BB ON 1 = 1
                AND AA.buycharactername = BB.charactername
                AND BB.dt = '[:DateNoLine]'
                AND BB.world = '[:World]'
            LEFT JOIN (
                SELECT 
                    commondata_8 AS itemid,
                    commondata_9 AS itemname,
                    commondata_10 AS sn
                FROM 
                    gtwpd.modelextract_modelextract BBB
                WHERE 1=1
                    AND game = 'maple'
                    AND tablenumber = '16091'
                    AND BBB.dt in (
                        SELECT max(dt) 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '16091'
                    )
            ) CC ON 1 = 1
                AND AA.commodityid = CC.sn
            WHERE 1 = 1
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.actionid IN (1,2,7,9) ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert16301DataSQL(self, makeInfo):
        # 非現金購買
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 16301
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]', dt='[:DateNoLine]',world='[:World]',tablenumber='16301')
            SELECT 
                LOWER(AA.nexonclubid) AS CommonData_1
                , AA.accountid   AS CommonData_2
                , AA.characterid AS CommonData_3
                , AA.buycharactername AS CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AA.commodityid as CommonData_8
                , NVL(CC.itemname,AA.commodityid) as CommonData_9
                , AA.itemid as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , 1  as UniqueInt_2
                , null as UniqueInt_3
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , AA.actionid  as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , 'bf point' as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , (CASE 
                    WHEN AA.actionid = 29 THEN '里程購買'
                    WHEN AA.actionid = 8 THEN '楓點購買'
                    END) as UniqueStr_11
                , null as UniqueStr_12
                , null as UniqueStr_13
                , null as UniqueStr_14
                , null as UniqueStr_15
                , null as UniqueStr_16
                , null as UniqueStr_17
                , null as UniqueStr_18
                , null as UniqueStr_19
                , null as UniqueStr_20
                , AA.price as uniquedbl_1 
                , AA.price as uniquedbl_2 
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
                , AA.registerdate as UniqueTime_1
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
                , array(null) as UniqueArray_01
                , array(null) as UniqueArray_02
                , null as UniqueJson_01
            FROM maple_extract.gamedb_cashitemlog AA
            LEFT JOIN (
                SELECT 
                    commondata_8 AS itemid,
                    commondata_9 AS itemname,
                    commondata_10 AS sn
                FROM 
                    gtwpd.modelextract_modelextract BBB
                WHERE 1=1
                    AND game = 'maple'
                    AND tablenumber = '16091'
                    AND BBB.dt in (
                        SELECT max(dt) 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '16091'
                    )
            ) CC ON 1 = 1
                AND AA.commodityid = CC.sn
            WHERE 1 = 1
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.actionid IN (8,29) ;
        """
        return "OrderInsert", [orderInsertSQLCode1]
    @classmethod
    def insert16401DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 16401
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]', dt='[:DateNoLine]',world='[:World]',tablenumber='16401')
            SELECT 
                LOWER(AA.nexonclubid) AS CommonData_1
                , AA.accountid   AS CommonData_2
                , AA.characterid AS CommonData_3
                , AA.buycharactername AS CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AA.commodityid as CommonData_8
                , NVL(CC.itemname,AA.commodityid) as CommonData_9
                , AA.itemid as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , 1  as UniqueInt_2
                , null as UniqueInt_3
                , null as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , AA.actionid as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , 'bf point' as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , '隨機箱產出' as UniqueStr_11
                , null as UniqueStr_12
                , null as UniqueStr_13
                , null as UniqueStr_14
                , null as UniqueStr_15
                , null as UniqueStr_16
                , null as UniqueStr_17
                , null as UniqueStr_18
                , null as UniqueStr_19
                , null as UniqueStr_20
                , AA.price as uniquedbl_1 
                , AA.price as uniquedbl_2 
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
                , AA.registerdate as UniqueTime_1
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
                , array(null) as UniqueArray_01
                , array(null) as UniqueArray_02
                , null as UniqueJson_01
            FROM maple_extract.gamedb_cashitemlog AA
            LEFT JOIN (
                SELECT 
                    commondata_8 AS itemid,
                    commondata_9 AS itemname,
                    commondata_10 AS sn
                FROM 
                    gtwpd.modelextract_modelextract BBB
                WHERE 1=1
                    AND game = 'maple'
                    AND tablenumber = '16091'
                    AND BBB.dt in (
                        SELECT max(dt) 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '16091'
                    )
            ) CC ON 1 = 1
                AND AA.commodityid = CC.sn
            WHERE 1 = 1
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.actionid = 16  ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert16451DataSQL(self, makeInfo):
        # 隨機箱產出對照
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 16451
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]', dt='[:DateNoLine]',world='[:World]',tablenumber='16451')
            SELECT 
                AAA.CommonData_1 AS CommonData_1
                , AAA.CommonData_2   AS CommonData_2
                , null AS CommonData_3
                , null AS CommonData_4
                , null as CommonData_5
                , CC.CommonData_9 as CommonData_6
                , CC.CommonData_10 as CommonData_7
                , AAA.CommonData_8 as CommonData_8
                , AAA.CommonData_9 as CommonData_9
                , AAA.CommonData_10 as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , CC.CommonData_8 as UniqueInt_1
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
                , CC.UniqueStr_1 as UniqueStr_1
                , CC.UniqueStr_2 as UniqueStr_2
                , CC.UniqueStr_3 as UniqueStr_3
                , CC.UniqueStr_4 as UniqueStr_4
                , CC.UniqueStr_5 as UniqueStr_5
                , CC.UniqueStr_6 as UniqueStr_6
                , CC.UniqueStr_7 as UniqueStr_7
                , CC.UniqueStr_8 as UniqueStr_8
                , CC.UniqueStr_9 as UniqueStr_9
                , CC.UniqueStr_10 as UniqueStr_10
                , CC.UniqueStr_11 as UniqueStr_11
                , CC.UniqueStr_12 as UniqueStr_12
                , CC.UniqueStr_13 as UniqueStr_13
                , CC.UniqueStr_14 as UniqueStr_14
                , CC.UniqueStr_15 as UniqueStr_15
                , CC.UniqueStr_16 as UniqueStr_16
                , CC.UniqueStr_17 as UniqueStr_17
                , CC.UniqueStr_18 as UniqueStr_18
                , CC.UniqueStr_19 as UniqueStr_19
                , null as UniqueStr_20
                , CC.uniquedbl_1 as uniquedbl_1 
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
                , AAA.UniqueTime_1 as UniqueTime_1
                , CC.UniqueTime_1 as UniqueTime_2
                , CC.UniqueTime_2 as UniqueTime_3
                , DDD.activity_time as otherstr_1 
                , null as otherstr_2 
                , null as otherstr_3 
                , null as otherstr_4 
                , null as otherstr_5 
                , null as otherstr_6 
                , null as otherstr_7 
                , null as otherstr_8 
                , null as otherstr_9 
                , null as otherstr_10
                , array(null) as UniqueArray_01
                , array(null) as UniqueArray_02
                , null as UniqueJson_01
            FROM  
            (
                SELECT 
                    AA.*
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1=1
                    AND AA.game = 'maple'
                    AND AA.tablenumber = '16401'
                    AND AA.dt = '[:DateNoLine]'
                    AND AA.world = '[:World]'
                    AND AA.commondata_9  = '時尚隨機箱'
            )AAA
            INNER JOIN 
            (
            SELECT 
                MIN(uniquetime_1 )  AS activity_time
            FROM 
                gtwpd.modelextract_modelextract BB
            WHERE 1=1 
                AND game = 'maple'
                AND tablenumber = '16092'
                AND BB.dt IN ( SELECT max(dt) as dt 
                                    FROM gtwpd.modelextract_modelextract 
                                    WHERE tablenumber = '16092' )
                AND uniquetime_1 <= '[:DateLine]'
                AND uniquetime_2 > '[:DateLine]'
            )DDD            
            LEFT JOIN 
            (
                SELECT 
                    * 
                FROM 
                    gtwpd.modelextract_modelextract BB
                WHERE 1=1 
                    AND game = 'maple'
                    AND tablenumber = '16092'
                    AND BB.dt IN ( SELECT max(dt) as dt 
                                        FROM gtwpd.modelextract_modelextract 
                                        WHERE tablenumber = '16092' )
            )CC
            ON 1=1
                AND AAA.commondata_10 = CC.CommonData_8
            WHERE 
                (
                    AAA.UniqueTime_1 >= CC.UniqueTime_1
                    AND AAA.UniqueTime_1 < CC.UniqueTime_2
                )
                OR CC.UniqueTime_1 IS NULL
                ;
        
        """
        return "OrderInsert", [orderInsertSQLCode1]

    def insert16452DataSQL(self, makeInfo):
        # 隨機箱後續使用
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 16452
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]', dt='[:DateNoLine]',world='[:World]',tablenumber='16452')
            SELECT 
                AAA.CommonData_1 AS CommonData_1
                , AAA.CommonData_2 AS CommonData_2
                , null AS CommonData_3
                , null AS CommonData_4
                , null as CommonData_5
                , MAX(CC.CommonData_9) as CommonData_6
                , MAX(CC.CommonData_10) as CommonData_7
                , MAX(AAA.CommonData_8) as CommonData_8
                , MAX(AAA.CommonData_9) as CommonData_9
                , MAX(AAA.CommonData_10) as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , MAX(CC.CommonData_8) as UniqueInt_1
                , MAX( CASE WHEN AAA.tablenumber = '3012' THEN 1 ELSE 0 END) as UniqueInt_2
                , MAX( CASE WHEN AAA.tablenumber = '16508' THEN 1 ELSE 0 END) as UniqueInt_3
                , MAX( CASE WHEN AAA.tablenumber = '16608' THEN 1 ELSE 0 END) as UniqueInt_4
                , MAX( CASE WHEN AAA.tablenumber = '16509' THEN 1 ELSE 0 END) as UniqueInt_5
                , MAX( CASE WHEN AAA.tablenumber = '16609' THEN 1 ELSE 0 END) as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , null as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , MAX(CC.UniqueStr_1)  as UniqueStr_1
                , MAX(CC.UniqueStr_2)  as UniqueStr_2
                , MAX(CC.UniqueStr_3)  as UniqueStr_3
                , MAX(CC.UniqueStr_4)  as UniqueStr_4
                , MAX(CC.UniqueStr_5)  as UniqueStr_5
                , MAX(CC.UniqueStr_6)  as UniqueStr_6
                , MAX(CC.UniqueStr_7)  as UniqueStr_7
                , MAX(CC.UniqueStr_8)  as UniqueStr_8
                , MAX(CC.UniqueStr_9)  as UniqueStr_9
                , MAX(CC.UniqueStr_10)  as UniqueStr_10
                , MAX(CC.UniqueStr_11)  as UniqueStr_11
                , MAX(CC.UniqueStr_12)  as UniqueStr_12
                , MAX(CC.UniqueStr_13)  as UniqueStr_13
                , MAX(CC.UniqueStr_14)  as UniqueStr_14
                , MAX(CC.UniqueStr_15)  as UniqueStr_15
                , MAX(CC.UniqueStr_16)  as UniqueStr_16
                , MAX(CC.UniqueStr_17)  as UniqueStr_17
                , MAX(CC.UniqueStr_18)  as UniqueStr_18
                , MAX(CC.UniqueStr_19)  as UniqueStr_19
                , null as UniqueStr_20
                , MAX(CC.uniquedbl_1) as uniquedbl_1 
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
                , MAX(AAA.UniqueTime_1) as UniqueTime_1
                , MAX(CC.UniqueTime_1) as UniqueTime_2
                , MAX(CC.UniqueTime_2) as UniqueTime_3
                , MAX(AAA.UniqueTime_1) as otherstr_1 
                , null as otherstr_2 
                , null as otherstr_3 
                , null as otherstr_4 
                , null as otherstr_5 
                , null as otherstr_6 
                , null as otherstr_7 
                , null as otherstr_8 
                , null as otherstr_9 
                , null as otherstr_10
                , array(null) as UniqueArray_01
                , array(null) as UniqueArray_02
                , null as UniqueJson_01
            FROM  
            (
                SELECT 
                    AA.*
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1=1
                    AND AA.game = 'maple'
                    AND AA.tablenumber IN ('3012','16508','16509','16608','16609')
                    AND AA.dt = '[:DateNoLine]'
                    AND AA.world = '[:World]'
            )AAA
            INNER JOIN 
            (
                SELECT 
                    * 
                FROM 
                    gtwpd.modelextract_modelextract BB
                WHERE 1=1 
                    AND game = 'maple'
                    AND tablenumber = '16092'
                    AND BB.dt IN ( SELECT max(dt) as dt 
                                        FROM gtwpd.modelextract_modelextract 
                                        WHERE tablenumber = '16092' )
                    AND uniquetime_1 <= '[:DateLine]'
                    AND uniquetime_2 > '[:DateLine]'
            )CC
            ON 1=1
            AND AAA.commondata_8 = CC.CommonData_8
            GROUP BY  AAA.CommonData_1,  AAA.CommonData_2 ,  AAA.CommonData_8
            ;

        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert16453DataSQL(self, makeInfo):
        # 隨機箱獲得方法
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 16453
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]', dt='[:DateNoLine]',world='[:World]',tablenumber='16453')
            SELECT 
                LOWER(AA.nexonclubid) AS CommonData_1
                , AA.accountid   AS CommonData_2
                , AA.characterid AS CommonData_3
                , AA.buycharactername AS CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AA.commodityid as CommonData_8
                , CC.itemname as CommonData_9
                , AA.itemid as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , 1  as UniqueInt_2
                , null as UniqueInt_3
                , AA.number as UniqueInt_4
                , null as UniqueInt_5
                , null as UniqueInt_6
                , null as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , AA.actionid as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                , 'bf point' as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , (CASE
                    WHEN AA.actionid = 1 THEN '購買'
                    WHEN AA.actionid IN (2,9) THEN '送禮'
                    WHEN AA.actionid = 29 THEN '里程購買'
                    END) as UniqueStr_11
                , null as UniqueStr_12
                , null as UniqueStr_13
                , null as UniqueStr_14
                , null as UniqueStr_15
                , null as UniqueStr_16
                , null as UniqueStr_17
                , null as UniqueStr_18
                , null as UniqueStr_19
                , null as UniqueStr_20
                , AA.price as uniquedbl_1 
                , AA.price as uniquedbl_2 
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
                , AA.registerdate as UniqueTime_1
                , DDD.activity_time as UniqueTime_2
                , null as UniqueTime_3
                , DDD.activity_time as otherstr_1 
                , null as otherstr_2 
                , null as otherstr_3 
                , null as otherstr_4 
                , null as otherstr_5 
                , null as otherstr_6 
                , null as otherstr_7 
                , null as otherstr_8 
                , null as otherstr_9 
                , null as otherstr_10
                , array(null) as UniqueArray_01
                , array(null) as UniqueArray_02
                , null as UniqueJson_01
            FROM maple_extract.gamedb_cashitemlog AA
            INNER JOIN 
            (
                SELECT 
                    MIN(uniquetime_1 )  AS activity_time
                FROM 
                    gtwpd.modelextract_modelextract BB
                WHERE 1=1 
                    AND game = 'maple'
                    AND tablenumber = '16092'
                    AND BB.dt IN ( SELECT max(dt) as dt 
                                        FROM gtwpd.modelextract_modelextract 
                                        WHERE tablenumber = '16092' )
                    AND uniquetime_1 <= '[:DateLine]'
                    AND uniquetime_2 > '[:DateLine]'
            )DDD            
            ON  1=1
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.actionid IN (1, 2,9,29) 
                AND AA.itemid IN (5222123 ,5680946)
            LEFT JOIN (
                SELECT 
                    commondata_8 AS itemid,
                    commondata_9 AS itemname,
                    commondata_10 AS sn
                FROM 
                    gtwpd.modelextract_modelextract BBB
                WHERE 1=1
                    AND game = 'maple'
                    AND tablenumber = '16091'
                    AND BBB.dt in (
                        SELECT max(dt) 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '16091'
                    )
            ) CC ON 1 = 1
                AND AA.commodityid = CC.sn
                ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert16508DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 16508 交易所(賣)
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='16508')
            SELECT
                CC.email as CommonData_1
                , BB.accountid  as CommonData_2
                , AA.stringacterid as CommonData_3
                , BB.charactername as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AA.itemid as CommonData_8
                , AA.itemid as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                -------------
                , AA.sn as UniqueInt_1
                , AA.auctionid as UniqueInt_2
                , AA.initprice as UniqueInt_3
                , AA.directprice as UniqueInt_4
                , AA.auctiontype as UniqueInt_5
                , AA.itemtype as UniqueInt_6
                , AA.state as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , null as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                -------------
                , null as UniqueStr_1
                , AA.biduserid as UniqueStr_2
                , DD.characterid as UniqueStr_3
                , AA.bidusername as UniqueStr_4
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
                -------------
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
                -------------
                , AA.tradedate as UniqueTime_1
                , AA.registerdate as UniqueTime_2
                , AA.enddate as UniqueTime_3
                -------------
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
                -------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.maple_middle_auctionhistory_log AA
            INNER JOIN maple_all.gamedb_character BB ON 1 = 1
                AND AA.stringacterid = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                AND BB.world = '[:World]'
            INNER JOIN maple_all.globalaccount_account CC ON 1 = 1
                AND BB.accountid = CC.accountid
                AND CC.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
            INNER JOIN maple_all.gamedb_character DD ON 1 = 1
                AND DD.charactername = AA.bidusername
                AND DD.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                AND DD.world = '[:World]'
            WHERE 1 = 1
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.initprice != 0 ;
            """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert16509DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 16509 交易所(買)
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='16509')
            SELECT
                CC.email as CommonData_1
                , BB.accountid  as CommonData_2
                , BB.characterid as CommonData_3
                , BB.charactername as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AA.itemid as CommonData_8
                , AA.itemid as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                -------------
                , AA.sn as UniqueInt_1
                , AA.auctionid as UniqueInt_2
                , AA.initprice as UniqueInt_3
                , AA.directprice as UniqueInt_4
                , AA.auctiontype as UniqueInt_5
                , AA.itemtype as UniqueInt_6
                , AA.state as UniqueInt_7
                , null as UniqueInt_8
                , null as UniqueInt_9
                , null as UniqueInt_10
                , null as UniqueInt_11
                , null as UniqueInt_12
                , null as UniqueInt_13
                , null as UniqueInt_14
                , null as UniqueInt_15
                -------------
                , null as UniqueStr_1
                , AA.accountid as UniqueStr_2
                , AA.stringacterid as UniqueStr_3
                , DD.charactername as UniqueStr_4
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
                -------------
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
                -------------
                , AA.tradedate as UniqueTime_1
                , AA.registerdate as UniqueTime_2
                , AA.enddate as UniqueTime_3
                -------------
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
                -------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.maple_middle_auctionhistory_log AA
            INNER JOIN maple_all.gamedb_character BB ON 1 = 1
                AND AA.bidusername = BB.charactername
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                AND BB.world = '[:World]'
            INNER JOIN maple_all.globalaccount_account CC ON 1 = 1
                AND BB.accountid = CC.accountid
                AND CC.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
            INNER JOIN maple_all.gamedb_character DD ON 1 = 1
                AND AA.stringacterid = DD.characterid
                AND DD.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                AND DD.world = '[:World]'
            WHERE 1 = 1
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.initprice != 0 ;
            """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert16608DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 16608 一對一(給)
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='16608')
            SELECT
                CC.email as CommonData_1
                , BB.accountid  as CommonData_2
                , AA.from_ as CommonData_3
                , BB.charactername as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AA.itemid as CommonData_8
                , AA.itemid as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                -------------
                , AA.itemsn as UniqueInt_1
                , AA.fieldid as UniqueInt_2
                , AA.through as UniqueInt_3
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
                -------------
                , null as UniqueStr_1
                , null as UniqueStr_2
                , AA.to_ as UniqueStr_3
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
                -------------
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
                -------------
                , AA.datetime_ as UniqueTime_1
                , null as UniqueTime_2
                , null as UniqueTime_3
                -------------
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
                -------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.maple_middle_itemmovepath_log AA
            INNER JOIN maple_all.gamedb_character BB ON 1 = 1
                AND AA.from_ = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                AND BB.world = '[:World]'
            INNER JOIN maple_all.globalaccount_account CC ON 1 = 1
                AND BB.accountid = CC.accountid
                AND CC.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
            WHERE 1 = 1
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                aND AA.type_ = 0 ;
            """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert16609DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 16609 一對一(收)
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='16609')
            SELECT
                CC.email as CommonData_1
                , BB.accountid  as CommonData_2
                , AA.to_ as CommonData_3
                , BB.charactername as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , AA.itemid as CommonData_8
                , AA.itemid as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                -------------
                , AA.itemsn as UniqueInt_1
                , AA.fieldid as UniqueInt_2
                , AA.through as UniqueInt_3
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
                -------------
                , null as UniqueStr_1
                , null as UniqueStr_2
                , AA.from_ as UniqueStr_3
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
                -------------
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
                -------------
                , AA.datetime_ as UniqueTime_1
                , null as UniqueTime_2
                , null as UniqueTime_3
                -------------
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
                -------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.maple_middle_itemmovepath_log AA
            INNER JOIN maple_all.gamedb_character BB ON 1 = 1
                AND AA.to_ = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                AND BB.world = '[:World]'
            INNER JOIN maple_all.globalaccount_account CC ON 1 = 1
                AND BB.accountid = CC.accountid
                AND CC.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
            WHERE 1 = 1
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                aND AA.type_ = 0 ;
            """
        return "OrderInsert", [orderInsertSQLCode1]

