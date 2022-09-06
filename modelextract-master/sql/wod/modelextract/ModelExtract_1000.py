import time
import datetime

class ModelExtract_1000 :

    @classmethod
    def insert1001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelData 1001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1001')
            SELECT 
                AAA.accountid as CommonData_1
                , AAA.accountid  as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , MAX(AAA.platformid) as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , CASE 
                    WHEN MAX(BBB.platformtype) = 4 THEN 'GUEST' 
                    WHEN MAX(BBB.platformtype) = 5 THEN 'FACEBOOK' 
                    WHEN MAX(BBB.platformtype) = 6 THEN 'APPLE'
                    WHEN MAX(BBB.platformtype) = 7 THEN 'BEANFUN'
                    ELSE 'OTHER' 
                  END as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , MAX(AAA.logincount) as UniqueInt_1
                , MAX(AAA.logoutcount) as UniqueInt_2
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
                , MAX(BBB.regdate) as UniqueTime_1
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
                    AA.accountid 
                    , MAX(AA.platformid) as platformid   
                    , sum(AA.logincount) AS logincount
                    , sum(AA.logoutcount) AS logoutcount
                FROM wod_extract.init_onliontimebyaccount AA
                WHERE 1 = 1
                    AND AA.dt = '[:DateNoLine]'
                GROUP BY
                    AA.accountid 
            ) AAA
            INNER JOIN wod_extract.account_t_user_platform BBB ON 1 = 1 
                AND BBB.dt = '[:DateNoLine]'
                AND AAA.platformid = BBB.platformuserid
            WHERE 1 = 1 
            GROUP BY
                AAA.accountid ;
        """
        return "OrderInsert" , [orderInsertSQLCode1]

    @classmethod
    def insert1002DataSQL(self,makeInfo):
        makeTime = makeInfo["makeTime"]
        print(makeTime)
        if makeTime >= datetime.datetime(2021, 4, 27, 0, 0, 0, 0):
            return self.__insert1002DataSQL_new_20210427(makeInfo)
        else:
            return self.__insert1002DataSQL_old_20201001_to_20210427(makeInfo)

    @classmethod
    def insert1003DataSQL(self,makeInfo):
        makeTime = makeInfo["makeTime"]
        print(makeTime)
        if makeTime >= datetime.datetime(2021, 4, 27, 0, 0, 0, 0):
            return self.__insert1003DataSQL_new_20210427(makeInfo)
        else:
            return self.__insert1003DataSQL_old_20201001_to_20210427(makeInfo)


    #====================================================================================================

    @classmethod
    def __insert1002DataSQL_new_20210427(self,makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelData 1002
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1002')
            SELECT
                AAA.accountid as CommonData_1
                , AAA.accountid  as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , MAX(AAA.platformid) as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , CASE 
                    WHEN MAX(BBB.platformtype) = 4 THEN 'GUEST' 
                    WHEN MAX(BBB.platformtype) = 5 THEN 'FACEBOOK' 
                    WHEN MAX(BBB.platformtype) = 6 THEN 'APPLE'
                    WHEN MAX(BBB.platformtype) = 7 THEN 'BEANFUN'
                    ELSE 'OTHER' 
                  END as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , MAX(AAA.logincount) as UniqueInt_1
                , MAX(AAA.logoutcount) as UniqueInt_2
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
                , MAX(CCC.regdate) as UniqueTime_1
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
            FROM wod_extract.init_onliontimebyaccount AAA
            INNER JOIN wod_extract.account_t_user_platform BBB ON 1 = 1 
                AND BBB.dt = '[:DateNoLine]'
                AND AAA.platformid = BBB.platformuserid
            INNER JOIN wod_extract.account_t_user CCC ON 1 = 1 
                AND CCC.dt = '[:DateNoLine]'
                AND AAA.accountid = CCC.userseq
            WHERE 1 = 1
                AND AAA.dt = '[:DateNoLine]'
            GROUP BY
                AAA.accountid ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def __insert1003DataSQL_new_20210427(self,makeInfo):
        fromSQLCode = """
            -- Make [:GameName] ModelData 1003
            FROM ( 
                SELECT
                    AA.accountid as CommonData_1
                    , AA.accountid  as CommonData_2
                    , AA.charid as CommonData_3
                    , AA.charname as CommonData_4
                    , MAX(AA.platformid) as CommonData_6
                    , max(AA.playertype) as CommonData_10
                    , sum(AA.logincount) as UniqueInt_1
                    , sum(AA.logoutcount) as UniqueInt_2
                    , MIN(BB.regdate) as UniqueTime_1
                    , AA.worldid as worldid
                FROM wod_extract.init_onliontime AA
                INNER JOIN wod_extract.game_t_pc BB ON 1 = 1 
                    AND BB.dt = '[:DateNoLine]'
                    AND AA.charid = BB.pcno
                WHERE 1 = 1
                    AND AA.dt = '[:DateNoLine]'
                GROUP BY
                    AA.accountid
                    , AA.charid
                    , AA.charname 
                    , AA.worldid
            ) AAA 
        """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1003')
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
                , AAA.UniqueTime_1 as UniqueTime_1
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
            WHERE 1 = 1 
        """
        return "MutiInsert", [fromSQLCode,mutiInsertSQLCode]

    @classmethod
    def __insert1002DataSQL_old_20201001_to_20210427(self,makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelData 1002
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1002')
            SELECT
                AAA.accountid as CommonData_1
                , AAA.accountid  as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , MAX(AAA.platformid) as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , CASE 
                    WHEN MAX(BBB.platformtype) = 4 THEN 'GUEST' 
                    WHEN MAX(BBB.platformtype) = 5 THEN 'FACEBOOK' 
                    WHEN MAX(BBB.platformtype) = 6 THEN 'APPLE'
                    WHEN MAX(BBB.platformtype) = 7 THEN 'BEANFUN'
                    ELSE 'OTHER' 
                  END as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , MAX(AAA.logincount) as UniqueInt_1
                , MAX(AAA.logoutcount) as UniqueInt_2
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
                , MAX(CCC.regdate) as UniqueTime_1
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
            FROM wod_extract.init_onliontimebyaccount AAA
            INNER JOIN wod_extract.account_t_user_platform BBB ON 1 = 1 
                AND BBB.dt = '[:DateNoLine]'
                AND AAA.platformid = BBB.platformuserid
            INNER JOIN wod_extract.account_t_user CCC ON 1 = 1 
                AND CCC.dt = '[:DateNoLine]'
                AND AAA.accountid = CCC.userseq
            WHERE 1 = 1
                AND AAA.dt = '[:DateNoLine]'
                AND ( 1 != 1
                    OR AAA.worldid = '[:World]' 
                    OR ( AAA.worldid = '111' AND '1011' = '[:World]' )  -- 1011的世界編號錯誤
                )
            GROUP BY
                AAA.accountid ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def __insert1003DataSQL_old_20201001_to_20210427(self,makeInfo):
        fromSQLCode = """
            -- Make [:GameName] ModelData 1003
            FROM ( 
                SELECT
                    AA.accountid as CommonData_1
                    , AA.accountid  as CommonData_2
                    , AA.charid as CommonData_3
                    , AA.charname as CommonData_4
                    , MAX(AA.platformid) as CommonData_6
                    , max(AA.playertype) as CommonData_10
                    , sum(AA.logincount) as UniqueInt_1
                    , sum(AA.logoutcount) as UniqueInt_2
                    , MIN(BB.regdate) as UniqueTime_1
                    , AA.worldid as worldid
                FROM wod_extract.init_onliontime AA
                INNER JOIN wod_extract.game_t_pc BB ON 1 = 1 
                    AND BB.dt = '[:DateNoLine]'
                    AND AA.charid = BB.pcno
                WHERE 1 = 1
                    AND AA.dt = '[:DateNoLine]'
                GROUP BY
                    AA.accountid
                    , AA.charid
                    , AA.charname 
                    , AA.worldid
            ) AAA 
        """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1003')
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
                , AAA.UniqueTime_1 as UniqueTime_1
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
            WHERE 1 = 1 
                AND ( 1 != 1
                    OR AAA.worldid = '[:World]' 
                    OR ( AAA.worldid = '111' AND '1011' = '[:World]' )  -- 1011的世界編號錯誤
                )
        """
        return "MutiInsert", [fromSQLCode,mutiInsertSQLCode]


