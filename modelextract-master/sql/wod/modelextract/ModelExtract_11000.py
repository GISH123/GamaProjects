import time

class ModelExtract_11000() :




    @classmethod
    def insert11802DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
               -- Make [:GameName] ME 11802
               INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION( game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11802')
               SELECT
                   AA.accountid as CommonData_1
                   , AA.accountid as CommonData_2
                   , null as CommonData_3
                   , null as CommonData_4
                   , null as CommonData_5
                   , MAX(AA.platformid) as CommonData_6
                    , null as CommonData_7
                    , null as CommonData_8
                    , null as CommonData_9
                    , CASE 
                        WHEN MAX(BB.platformtype) = 4 THEN 'GUEST' 
                        WHEN MAX(BB.platformtype) = 5 THEN 'FACEBOOK' 
                        WHEN MAX(BB.platformtype) = 6 THEN 'APPLE'
                        WHEN MAX(BB.platformtype) = 7 THEN 'BEANFUN'
                        ELSE 'OTHER' 
                      END as CommonData_10
                   , null as CommonData_11
                   , null as CommonData_12
                   , null as CommonData_13
                   , null as CommonData_14
                   , null as CommonData_15
                   , cast(split(split(AA.accessip,':')[0],'\\\\.')[0] as bigint)*256*256*256
                       + cast(split(split(AA.accessip,':')[0],'\\\\.')[1] as bigint)*256*256
                       + cast(split(split(AA.accessip,':')[0],'\\\\.')[2] as bigint)*256
                       + cast(split(split(AA.accessip,':')[0],'\\\\.')[3] as bigint) as UniqueInt_1
                   , COUNT(AA.accountid) as UniqueInt_2
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
                   , AA.accessip as UniqueStr_1
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
                   , array(null) as UniqueArray_01
                   , array(null) as UniqueArray_02
                   , null as UniqueJson_01
               FROM wod_extract.init_onliontimebyaccount AA
               INNER JOIN wod_extract.account_t_user_platform BB ON 1 = 1 
                    AND BB.dt = '[:DateNoLine]'
                    AND AA.platformid = BB.platformuserid
               WHERE 1 = 1
                    AND AA.dt = '[:DateNoLine]'
               GROUP BY
                    AA.accountid
                   , AA.accessip  ;
           """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert11803DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
               -- Make [:GameName] ME 11083
               INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]', dt='[:DateNoLine]',world='[:World]',tablenumber='11803')
               SELECT
                   AA.accountid as CommonData_1
                   , AA.accountid as CommonData_2
                   , AA.charid as CommonData_3
                   , AA.charname as CommonData_4
                   , null as CommonData_5
                   , MAX(AA.platformid) as CommonData_6
                    , null as CommonData_7
                    , null as CommonData_8
                    , null as CommonData_9
                    , CASE 
                        WHEN MAX(BB.platformtype) = 4 THEN 'GUEST' 
                        WHEN MAX(BB.platformtype) = 5 THEN 'FACEBOOK' 
                        WHEN MAX(BB.platformtype) = 6 THEN 'APPLE'
                        WHEN MAX(BB.platformtype) = 7 THEN 'BEANFUN'
                        ELSE 'OTHER' 
                      END as CommonData_10
                   , null as CommonData_11
                   , null as CommonData_12
                   , null as CommonData_13
                   , null as CommonData_14
                   , null as CommonData_15
                   , cast(split(split(AA.accessip,':')[0],'\\\\.')[0] as bigint)*256*256*256
                       + cast(split(split(AA.accessip,':')[0],'\\\\.')[1] as bigint)*256*256
                       + cast(split(split(AA.accessip,':')[0],'\\\\.')[2] as bigint)*256
                       + cast(split(split(AA.accessip,':')[0],'\\\\.')[3] as bigint) as UniqueInt_1
                   , COUNT(AA.charid) as UniqueInt_2
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
                   , AA.accessip as UniqueStr_1
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
                   , array(null) as UniqueArray_01
                   , array(null) as UniqueArray_02
                   , null as UniqueJson_01
               FROM wod_extract.init_onliontime AA
               INNER JOIN wod_extract.account_t_user_platform BB ON 1 = 1 
                    AND BB.dt = '[:DateNoLine]'
                    AND AA.platformid = BB.platformuserid
               WHERE 1 = 1
                   AND AA.dt = '[:DateNoLine]'
                   AND ( 1 != 1
                      OR AA.worldid = '[:World]' 
                      OR ( AA.worldid = '111' AND '1011' = '[:World]' )  -- 1011的世界編號錯誤
                      OR '[:DateLine]' >= '2021-04-27'                      -- 2021-04-27以後的LOG編號錯誤
                   )
               GROUP BY
                   AA.accountid
                   , AA.charid
                   , AA.charname
                   , AA.accessip  ;
           """
        return "OrderInsert", [orderInsertSQLCode1]


