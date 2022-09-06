import time

class ModelExtract_13000() :

    @classmethod
    def insert13051DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 13051 是否裝備防掉%裝備
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='13051')
            SELECT
                max(AAA.serviceid) AS CommonData_1
                , max(AAA.accountid) as CommonData_2
                , AAA.characterid as CommonData_3
                , max(AAA.charactername) as CommonData_4
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
                , MAX(AAA.item_1662072) AS UniqueInt_1
                , MAX(AAA.item_1662073) AS UniqueInt_2
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
                ----------------------------------------
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
                ----------------------------------------
                , null as UniqueTime_1
                , null as UniqueTime_2
                , null as UniqueTime_3
                ----------------------------------------
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
                ----------------------------------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM (
                SELECT
                    AA.CommonData_1 as serviceid
                    , AA.CommonData_2  as accountid
                    , AA.CommonData_3 as characterid
                    , AA.CommonData_4 as charactername
                    , 1 AS islogin
                    , 0 AS item_1662072
                    , 0 AS item_1662073
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game='[:GameName]'
                    AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND AA.world = '[:World]'
                    AND AA.tablenumber = 11003
                UNION aLL
                SELECT
                    null as serviceid
                    , null  as accountid
                    , AA.characterid as characterid
                    , NULL as charactername
                    , 0 AS islogin
                    , MAX(CASE WHEN AA.itemid = 1662072 THEN 1 ELSE 0 END ) AS item_1662072
                    , MAX(CASE WHEN AA.itemid = 1662073 THEN 1 ELSE 0 END ) AS item_1662073
                FROM maple_extract.gamedb_itemslot_eqp AA
                WHERE 1 = 1
                    AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND AA.world = '[:World]'
                    AND AA.itemid IN ( 1662072 , 1662073 )
                    AND AA.pos < 0
                GROUP BY
                    characterid
            ) AAA
            GROUP BY
                AAA.characterid
            HAVING
                MAX(AAA.islogin) == 1 ;           
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert13052DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 13052 是否有經驗加倍道具
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='13052')
            SELECT
                null as CommonData_1
                , null as CommonData_2
                , AAA.characterid as CommonData_3
                , null as CommonData_4
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
                , AAA.itemid as UniqueInt_1
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
                ----------------------------------------
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
                ----------------------------------------
                , CASE
                    WHEN AAA.startdate <= '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00'
                    ELSE DATE_FORMAT(AAA.startdate,'yyyy-MM-dd HH:mm:ss')
                END as UniqueTime_1 --startdate
                , CASE
                    WHEN AAA.enddate >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59'
                    ELSE DATE_FORMAT(AAA.enddate,'yyyy-MM-dd HH:mm:ss')
                END as UniqueTime_2 --enddate
                , null as UniqueTime_3
                ----------------------------------------
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
                ----------------------------------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM (
                SELECT
                    AA.dt
                    , AA.characterid
                    , AA.itemid
                    , AA.registerdate AS startdate
                    , CASE
                        WHEN AA.itemid in (5680644,5680645,5680646) THEN from_unixtime(unix_timestamp(AA.registerdate)+15*24*60*60)
                        WHEN AA.itemid in (5680641,5680642,5680643) THEN from_unixtime(unix_timestamp(AA.registerdate)+1*24*60*60)
                    END AS enddate
                FROM maple_extract.gamedb_cashitemlog AA
                WHERE 1 = 1
                    AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-16),'yyyyMMdd')
                    AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND AA.registerdate >= DATE_ADD('[:DateLine]',-16)
                    AND AA.registerdate < DATE_ADD('[:DateLine]',1)
                    AND AA.itemid in (5680641,5680642,5680643,5680644,5680645,5680646)
                    AND AA.actionid = 3
            ) AAA
            WHERE 1 = 1
                AND AAA.enddate >= DATE_ADD('[:DateLine]',0)
                AND AAA.startdate < DATE_ADD('[:DateLine]',1) ;            
        """
        return "OrderInsert", [orderInsertSQLCode1]


    @classmethod
    def insert13053DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 13053 全玩家時裝資料
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='13053')
            SELECT
                  AA.CommonData_1 as CommonData_1
                , AA.CommonData_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
                , AA.CommonData_5 as CommonData_5
                , AA.CommonData_6 as CommonData_6
                , AA.CommonData_7 as CommonData_7
                , NULL as CommonData_8
                , NULL as CommonData_9
                , BB.itemname as CommonData_10
                , AA.CommonData_8 as CommonData_11
                , NULL as CommonData_12
                , NULL as CommonData_13
                , NULL as CommonData_14
                , NULL as CommonData_15
                , DD.cluster as UniqueInt_1
                , DD.id as UniqueInt_2
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
                , DD.type as UniqueStr_1
                , DD.desc as UniqueStr_2
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
                ----------------------------------------
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
                ----------------------------------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM
            (
                SELECT 
                    * 
                FROM 
                    gtwpd.modelextract_modelextract
                WHERE 1=1
                    AND game = 'maple'
                    AND tablenumber = '3012'
                    AND dt = [:DateNoLine]
                    AND world='[:World]'
                    AND UniqueStr_4 <0 
            )AA
            INNER JOIN
            (
                SELECT 
                    uniqueint_1 AS itemid,
                    uniquestr_1 AS itemname
                FROM 
                    gtwpd.modelextract_modelextract BBB
                WHERE 1=1
                    AND game = 'maple'
                    AND tablenumber = '13091'
                    AND BBB.dt in (
                        SELECT max(dt) 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '13091'
                    )	
            )BB
            ON AA.CommonData_8 = BB.itemid
            INNER JOIN
            (
                SELECT 
                    * 
                FROM 
                    gtwpd.maple_resultrua_group
                WHERE 1=1
                    AND dt = [:DateNoLineFirstPrevMonth]
                    AND world='[:World]'
            )CC
            ON AA.CommonData_2 = CC.accountid
            AND AA.CommonData_3 = CC.characterid
            AND AA.world = CC.world
            INNER JOIN 
                gtwpd.maple_othertable_acctype DD
            ON CC.cluster = DD.cluster;      
        """
        return "OrderInsert", [orderInsertSQLCode1]


    @classmethod
    def insert13054DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 13054 全玩家時裝資料
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='13054')
            SELECT
                  AA.CommonData_1 as CommonData_1
                , AA.CommonData_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
                , AA.CommonData_5 as CommonData_5
                , AA.CommonData_6 as CommonData_6
                , AA.CommonData_7 as CommonData_7
                , NULL as CommonData_8
                , NULL as CommonData_9
                , BB.itemname as CommonData_10
                , AA.CommonData_8 as CommonData_11
                , NULL as CommonData_12
                , NULL as CommonData_13
                , NULL as CommonData_14
                , NULL as CommonData_15
                , DD.cluster as UniqueInt_1
                , DD.id as UniqueInt_2
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
                , DD.type as UniqueStr_1
                , DD.desc as UniqueStr_2
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
                ----------------------------------------
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
                ----------------------------------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM
            (
                SELECT 
                    * 
                FROM 
                    gtwpd.modelextract_modelextract
                WHERE 1=1
                    AND game = 'maple'
                    AND tablenumber = '3012'
                    AND dt = [:DateNoLine]
                    AND world='[:World]'
                    AND UniqueStr_4 <0 
            )AA
            INNER JOIN
            (
                SELECT 
                    uniqueint_1 AS itemid,
                    uniquestr_1 AS itemname
                FROM 
                    gtwpd.modelextract_modelextract BBB
                WHERE 1=1
                    AND game = 'maple'
                    AND tablenumber = '13092'
                    AND BBB.dt in (
                        SELECT max(dt) 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '13092'
                    )	
            )BB
            ON AA.CommonData_8 = BB.itemid
            INNER JOIN
            (
                SELECT 
                    * 
                FROM 
                    gtwpd.maple_resultrua_group
                WHERE 1=1
                    AND dt = [:DateNoLineFirstPrevMonth]
                    AND world='[:World]'
            )CC
            ON AA.CommonData_2 = CC.accountid
            AND AA.CommonData_3 = CC.characterid
            AND AA.world = CC.world
            INNER JOIN 
                gtwpd.maple_othertable_acctype DD
            ON CC.cluster = DD.cluster;      
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert13056DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 13056 全玩家時裝對應bf活動道具
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='13056')
            SELECT
                  AA.CommonData_1 as CommonData_1
                , AA.CommonData_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
                , AA.CommonData_5 as CommonData_5
                , AA.CommonData_6 as CommonData_6
                , AA.CommonData_7 as CommonData_7
                , BB.itemid as CommonData_8
                , NULL as CommonData_9
                , BB.color as CommonData_10
                , NULL as CommonData_11
                , NULL as CommonData_12
                , NULL as CommonData_13
                , NULL as CommonData_14
                , NULL as CommonData_15
                , NULL as UniqueInt_1
                , NULL as UniqueInt_2
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
                , BB.color as UniqueStr_1
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
                ----------------------------------------
                , BB.weight as uniquedbl_1
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
                ----------------------------------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM
            (
                SELECT 
                    * 
                FROM 
                    gtwpd.modelextract_modelextract
                WHERE 1=1
                    AND game = 'maple'
                    AND tablenumber = '3012'
                    AND dt = [:DateNoLine]
                    AND world='[:World]'
                    AND UniqueStr_4 <0 
            )AA
            INNER JOIN
            (
                SELECT 
                    commondata_8 AS itemid,
                    MAX(uniquestr_1) AS itemType,
                    uniquestr_4 AS color,
                    SUM(uniquedbl_1) AS weight
                FROM 
                    gtwpd.modelextract_modelextract BBB
                WHERE 1=1
                    AND game = 'maple'
                    AND tablenumber = '13096'
                    AND uniquestr_4 != ''
                    AND BBB.dt in (
                        SELECT max(dt) 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '13096'
                        AND game = 'maple'
                    )
                GROUP BY commondata_8, uniquestr_4
            )BB
            ON AA.CommonData_8 = BB.itemid;
        """
        return "OrderInsert", [orderInsertSQLCode1]

