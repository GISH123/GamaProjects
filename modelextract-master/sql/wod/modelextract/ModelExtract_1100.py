import time
from sql.common.modelextract.ModelExtract_1100 import ModelExtract_1100 as ModelExtract_1100_Common

class ModelExtract_1100(ModelExtract_1100_Common) :

    @classmethod
    def insert1101DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1101
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1101')
            SELECT
                AAA.accountid as CommonData_1
                , AAA.accountid  as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , AAA.platformid as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , CASE 
                    WHEN BBB.platformtype = 4 THEN 'GUEST' 
                    WHEN BBB.platformtype = 5 THEN 'FACEBOOK' 
                    WHEN BBB.platformtype = 6 THEN 'APPLE'
                    WHEN BBB.platformtype = 7 THEN 'BEANFUN'
                    ELSE 'OTHER' 
                  END as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , AAA.logincount as UniqueInt_1
                , AAA.logoutcount as UniqueInt_2
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
                , null as UniqueStr_11
                , null as UniqueStr_12
                , null as UniqueStr_13
                , null as UniqueStr_14
                , null as UniqueStr_15
                , null as UniqueStr_16
                , null as UniqueStr_17
                , null as UniqueStr_18
                , null as UniqueStr_19
                , null as UniqueStr_20
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
                , AAA.logintime as UniqueTime_1
                , AAA.logouttime as UniqueTime_2
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
            FROM wod_extract.init_onliontimebyaccount AAA
            INNER JOIN wod_extract.account_t_user_platform BBB ON 1 = 1 
                AND BBB.dt = '[:DateNoLine]'
                AND AAA.platformid = BBB.platformuserid
            WHERE 1 = 1
                AND AAA.dt = '[:DateNoLine]' ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1102DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1102
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1102')
            SELECT
                AAA.accountid as CommonData_1
                , AAA.accountid  as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , AAA.platformid as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , CASE 
                    WHEN BBB.platformtype = 4 THEN 'GUEST' 
                    WHEN BBB.platformtype = 5 THEN 'FACEBOOK' 
                    WHEN BBB.platformtype = 6 THEN 'APPLE'
                    WHEN BBB.platformtype = 7 THEN 'BEANFUN'
                    ELSE 'OTHER' 
                  END as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , AAA.logincount as UniqueInt_1
                , AAA.logoutcount as UniqueInt_2
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
                , null as UniqueStr_11
                , null as UniqueStr_12
                , null as UniqueStr_13
                , null as UniqueStr_14
                , null as UniqueStr_15
                , null as UniqueStr_16
                , null as UniqueStr_17
                , null as UniqueStr_18
                , null as UniqueStr_19
                , null as UniqueStr_20
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
                , AAA.logintime as UniqueTime_1
                , AAA.logouttime as UniqueTime_2
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
            FROM wod_extract.init_onliontimebyaccount AAA
            INNER JOIN wod_extract.account_t_user_platform BBB ON 1 = 1 
                AND BBB.dt = '[:DateNoLine]'
                AND AAA.platformid = BBB.platformuserid
            WHERE 1 = 1
                AND AAA.dt = '[:DateNoLine]' 
                AND ( 1 != 1
                     OR AAA.worldid = '[:World]' 
                     OR ( AAA.worldid = '111' AND '1011' = '[:World]' )  -- 1011的世界編號錯誤
                     OR '[:DateLine]' >= '2021-04-27'                      -- 2021-04-27以後的LOG編號錯誤
                )
                ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1103DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] ModelData 1103
            FROM ( 
                SELECT
                    AA.accountid as CommonData_1
                    , AA.accountid  as CommonData_2
                    , AA.charid as CommonData_3
                    , AA.charname as CommonData_4
                    , AA.platformid as CommonData_6
                    , AA.playertype as CommonData_10
                    , AA.logincount as UniqueInt_1
                    , AA.logoutcount as UniqueInt_2
                    , AA.logintime as UniqueTime_1
                    , AA.logouttime as UniqueTime_2
                    , AA.worldid as worldid
                FROM wod_extract.init_onliontime AA
                WHERE 1 = 1
                    AND AA.dt = '[:DateNoLine]'
            ) AAA 
        """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1103')
            SELECT 
                AAA.CommonData_1 
                , AAA.CommonData_2  
                , AAA.CommonData_3 
                , AAA.CommonData_4 
                , null as CommonData_5
                , AAA.CommonData_6 
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , AAA.CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , AAA.UniqueInt_1
                , AAA.UniqueInt_2
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
                , null as UniqueStr_11
                , null as UniqueStr_12
                , null as UniqueStr_13
                , null as UniqueStr_14
                , null as UniqueStr_15
                , null as UniqueStr_16
                , null as UniqueStr_17
                , null as UniqueStr_18
                , null as UniqueStr_19
                , null as UniqueStr_20
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
                , AAA.UniqueTime_1
                , AAA.UniqueTime_2
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
            WHERE 1 = 1 
               AND ( 1 != 1
                     OR AAA.worldid = '[:World]' 
                     OR ( AAA.worldid = '111' AND '1011' = '[:World]' )  -- 1011的世界編號錯誤
                     OR '[:DateLine]' >= '2021-04-27'                      -- 2021-04-27以後的LOG編號錯誤
               ) 
        """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert1131DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
             -- Make [:GameName] ME 1131
             INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1131')
             SELECT
                 null as CommonData_1
                 , null as CommonData_2
                 , null as CommonData_3
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
                 , sum(AAAA.ind) OVER (ORDER BY AAAA.logtime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as UniqueInt_1
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
                , null as UniqueStr_11
                , null as UniqueStr_12
                , null as UniqueStr_13
                , null as UniqueStr_14
                , null as UniqueStr_15
                , null as UniqueStr_16
                , null as UniqueStr_17
                , null as UniqueStr_18
                , null as UniqueStr_19
                , null as UniqueStr_20
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
                , AAAA.logtime as UniqueTime_1
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
                     AAA.logtime
                     , sum(AAA.ind) AS ind
                 FROM (
                     SELECT
                         date_format(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss') AS logtime
                         , SUM(1) as ind
                     FROM gtwpd.modelextract_modelextract AA
                     WHERE 1 = 1
                         AND AA.game = '[:GameName]'
                         AND AA.dt = '[:DateNoLine]'
                         AND AA.world = '[:World]'
                         AND AA.tablenumber = '1103'
                     GROUP BY
                         date_format(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                     UNION ALL
                     SELECT
                         date_format(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss') AS logtime
                         , SUM(-1) AS ind
                     FROM gtwpd.modelextract_modelextract AA
                     WHERE 1 = 1
                         AND AA.game = '[:GameName]'
                         AND AA.dt = '[:DateNoLine]'
                         AND AA.world = '[:World]'
                         AND AA.tablenumber = '1103'
                         AND date_format(AA.uniquetime_2,'HH:mm:ss') != '23:59:59'
                     GROUP BY
                         date_format(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                     UNION ALL
                     SELECT
                         concat('[:DateLine] ',uniquestr_1) AS logtime
                         , 0 AS ind
                     FROM gtwpd.modelextract_modelextract AA
                     WHERE 1 = 1
                         AND AA.tablenumber = 90001
                 ) AAA
                 GROUP BY
                     AAA.logtime
             ) AAAA ;
         """
        return "OrderInsert", [orderInsertSQLCode1]

