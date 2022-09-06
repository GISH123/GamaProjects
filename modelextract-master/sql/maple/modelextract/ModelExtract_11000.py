import time
import datetime


class ModelExtract_11000() :

    @classmethod
    def insert11002DataSQL(self, makeInfo):
        makeTime = makeInfo["makeTime"]
        print(makeTime)
        if makeTime >= datetime.datetime(2020, 12, 2, 0, 0, 0, 0)  :
            return self.__insert11002DataSQL_new_20201202(makeInfo)
        else :
            return self.__insert11002DataSQL_old_20191201_to_20201201(makeInfo)

    @classmethod
    def insert11003DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 11003
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION( game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='11003')
            SELECT
                MAX(lower(AAAA.email))  as CommonData_1
                , MAX(BBBB.accountid) as CommonData_2
                , BBBB.characterid as CommonData_3
                , MAX(BBBB.charactername) as CommonData_4
                , MAX(lower(DDDD.mainaccount)) as CommonData_5
                , MAX(lower(EEEE.globalsn)) as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
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
                , MAX(BBBB.registerdate) as UniqueTime_1
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
            FROM (
                SELECT
                    AA.accountid
                    , AA.charactername
                    , lower(AA.email) as email
                FROM gtwpd.maple_middle_login_record AA
                WHERE 1 = 1
                    AND AA.dt = '[:DateNoLine]'
                    AND AA.world = '[:World]'
            ) AAAA
            INNER JOIN (
                SELECT
                    BB.accountid
                    , BB.characterid
                    , BB.charactername
                    , CC.registerdate
                FROM maple_extract.gamedb_character BB
                LEFT JOIN maple_extract.globalaccount_character CC ON 1 = 1
                    AND BB.accountid = CC.accountid
                    AND BB.characterid = CC.characterid
                    AND CC.dt = '[:DateNoLine]'
                WHERE 1 = 1
                    AND BB.dt = '[:DateNoLine]'
                    AND BB.world = '[:World]'
            ) BBBB ON 1 = 1
                AND AAAA.accountid = BBBB.accountid
                AND AAAA.charactername = BBBB.charactername
            INNER JOIN (
                SELECT
                    CC.accountid
                    , CC.registerdate
                FROM maple_extract.globalaccount_account CC
                WHERE 1 = 1
                    AND CC.dt = '[:DateNoLine]'
            ) CCCC ON 1 = 1
                AND AAAA.accountid = CCCC.accountid
            LEFT JOIN (
                SELECT
                    DDD.serviceaccountid
                    , MAX(DDD.mainaccount) as mainaccount
                FROM (
                    SELECT
                        EE.commondata_1 as serviceaccountid
                        , FIRST_VALUE(EE.commondata_5) over (partition by EE.commondata_1 order by dt desc ) as mainaccount
                    FROM gtwpd.modelextract_modelextract EE
                    WHERE 1=1
                        AND EE.tablenumber = '10001'
                        AND EE.commondata_8 = '610074'
                        AND not ( not 1 != 1
                            and not ('[:DateNoLine]' < '20200701' AND EE.dt = '20200701')
                            and not ( 1 = 1
                                AND '[:DateNoLine]' >= '20200701'
                                AND EE.dt >= '[:DateNoLine]'
                                AND EE.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',2),'yyyyMMdd'))
                        )
                ) DDD
                GROUP BY
                    DDD.serviceaccountid
            ) DDDD ON 1 = 1
                AND lower(AAAA.email) = lower(DDDD.serviceaccountid)
            LEFT JOIN (
                SELECT
                    DISTINCT EEE.mainaccountid
                    , EEE.globalsn
                FROM (
                    SELECT
                        commondata_5 as mainaccountid
                        , FIRST_VALUE(EE.commondata_6) over (partition by EE.commondata_5 order by EE.dt desc ) as globalsn
                    FROM gtwpd.modelextract_modelextract EE
                    WHERE 1=1
                        AND EE.tablenumber = '10001'
                        AND EE.commondata_8 = '610074'
                        AND not ( not 1 != 1
                            and not ('[:DateNoLine]' < '20200701' AND EE.dt = '20200701')
                            and not ( 1 = 1
                                AND '[:DateNoLine]' >= '20200701'
                                AND EE.dt >= '[:DateNoLine]'
                                AND EE.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',2),'yyyyMMdd'))
                        )
                ) EEE
            ) EEEE ON 1 = 1
                AND lower(DDDD.mainaccount) = lower(EEEE.mainaccountid)
            GROUP BY
                BBBB.characterid ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert11102DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 11102
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION( game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11102')
            Select
                null as CommonData_1
                , AA.accountid as CommonData_2
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
                , AA.login_time as UniqueTime_1
                , AA.logout_time as UniqueTime_2
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
            FROM gtwpd.maple_middle_login_record AA
            WHERE 1 = 1
                AND AA.dt = '[:DateNoLine]' ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert11103DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 11103
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION( game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='11103')
            Select
                null as CommonData_1
                , null as CommonData_2
                , BBB.characterid  as CommonData_3
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
                , null as UniqueStr_1 --as newbeanfunbindaccount
                , null as UniqueStr_2 --as mainaccountid
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
                , AAA.login_time as UniqueTime_1
                , AAA.logout_time  as UniqueTime_2
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
            FROM (
                SELECT
                    AA.accountid
                    , AA.charactername
                    , AA.login_time
                    , AA.logout_time
                    , AA.email
                FROM gtwpd.maple_middle_login_record AA
                WHERE 1 = 1
                    AND AA.dt = '[:DateNoLine]'
                    AND AA.world = '[:World]'
            ) AAA
            INNER JOIN (
                SELECT
                    BB.accountid
                    , BB.characterid
                    , BB.charactername
                FROM maple_extract.gamedb_character BB
                WHERE 1 = 1
                    AND BB.dt = '[:DateNoLine]'
                    AND BB.world = '[:World]'
            ) BBB ON 1 = 1
                AND AAA.accountid = BBB.accountid
                AND AAA.charactername = BBB.charactername ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert11802DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
               -- Make [:GameName] ME 11802
               INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION( game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11802')
               SELECT
                   lower(AA.email) as CommonData_1
                   , AA.accountid as CommonData_2
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
                   , cast(split(split(AA.ip,':')[0],'\\\\.')[0] as bigint)*256*256*256
                       + cast(split(split(AA.ip,':')[0],'\\\\.')[1] as bigint)*256*256
                       + cast(split(split(AA.ip,':')[0],'\\\\.')[2] as bigint)*256
                       + cast(split(split(AA.ip,':')[0],'\\\\.')[3] as bigint) as UniqueInt_1
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
                   , split(AA.ip,':')[0] as UniqueStr_1
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
               FROM gtwpd.maple_middle_login_record AA
               WHERE 1 = 1
                   AND AA.dt = '[:DateNoLine]'
                   AND AA.login_type like 'IN%'
               GROUP BY
                   lower(AA.email)
                   , AA.accountid
                   , split(AA.ip,':')[0] ;
           """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert11803DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
               -- Make [:GameName] ME 11083
               INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]', dt='[:DateNoLine]',world='[:World]',tablenumber='11803')
               SELECT
                   null as CommonData_1
                   , null as CommonData_2
                   , BB.characterid as CommonData_3
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
                   , cast(split(split(AA.ip,':')[0],'\\\\.')[0] as bigint)*256*256*256
                       + cast(split(split(AA.ip,':')[0],'\\\\.')[1] as bigint)*256*256
                       + cast(split(split(AA.ip,':')[0],'\\\\.')[2] as bigint)*256
                       + cast(split(split(AA.ip,':')[0],'\\\\.')[3] as bigint) as UniqueInt_1
                   , COUNT(BB.characterid) as UniqueInt_2
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
                   , split(AA.ip,':')[0] as UniqueStr_1
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
               FROM gtwpd.maple_middle_login_record AA
               INNER JOIN maple_extract.gamedb_character BB ON 1 = 1
                   AND AA.accountid = BB.accountid
                   AND AA.charactername = BB.charactername
                   AND BB.dt = '[:DateNoLine]'
                   AND BB.world = '[:World]'
               WHERE 1 = 1
                   AND AA.dt = '[:DateNoLine]'
                   AND AA.login_type like 'IN%'
               GROUP BY
                   BB.characterid
                   , split(AA.ip,':')[0] ;
           """
        return "OrderInsert", [orderInsertSQLCode1]

    #=================================================================================================

    @classmethod
    def __insert11002DataSQL_new_20201202(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 11002
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION( game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11002')
            SELECT
                MAX(lower(AAAA.email)) as CommonData_1 --as accountid
                , AAAA.accountid as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , MAX(lower(CCCC.mainaccount)) as CommonData_5
                , MAX(lower(DDDD.globalsn)) as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
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
                , null as UniqueStr_1 --as newbeanfunbindaccount
                , null as UniqueStr_2 --as mainaccountid
                , null as UniqueStr_3 --as serviceaccountid
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
                , MAX(BBBB.registerdate) as UniqueTime_1
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
            FROM (
                SELECT
                    AA.accountid
                    , lower(AA.email) as email
                FROM gtwpd.maple_middle_login_record AA
                WHERE 1 = 1
                    AND AA.dt = '[:DateNoLine]'
            ) AAAA
            INNER JOIN (
                SELECT
                    BB.accountid
                    , BB.registerdate
                FROM maple_extract.globalaccount_account BB
                WHERE 1 = 1
                    AND BB.dt = '[:DateNoLine]'
            ) BBBB ON 1 = 1
                AND AAAA.accountid = BBBB.accountid
            LEFT JOIN (
                SELECT
                    DISTINCT CCC.serviceaccountid
                    , CCC.mainaccount
                FROM (
                    SELECT
                        EE.commondata_1 as serviceaccountid
                        , FIRST_VALUE(EE.commondata_5) over (partition by EE.commondata_1 order by dt desc ) as mainaccount
                    FROM gtwpd.modelextract_modelextract EE
                    WHERE 1=1
                        AND EE.tablenumber = '10001'
                        AND EE.commondata_8 = '610074'
                        AND not ( not 1 != 1
                            and not ('[:DateNoLine]' < '20200701' AND EE.dt = '20200701')
                            and not ( 1 = 1
                                AND '[:DateNoLine]' >= '20200701'
                                AND EE.dt >= '[:DateNoLine]'
                                AND EE.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',2),'yyyyMMdd'))
                        )
                ) CCC
            ) CCCC ON 1 = 1
                AND lower(AAAA.email) = lower(CCCC.serviceaccountid)
            LEFT JOIN (
                SELECT
                    DISTINCT DDD.mainaccountid
                    , DDD.globalsn
                FROM (
                    SELECT
                        commondata_5 as mainaccountid
                        , FIRST_VALUE(EE.commondata_6) over (partition by EE.commondata_5 order by EE.dt desc ) as globalsn
                    FROM gtwpd.modelextract_modelextract EE
                    WHERE 1=1
                        AND EE.tablenumber = '10001'
                        AND EE.commondata_8 = '610074'
                        AND not ( not 1 != 1
                            and not ('[:DateNoLine]' < '20200701' AND EE.dt = '20200701')
                            and not ( 1 = 1
                                AND '[:DateNoLine]' >= '20200701'
                                AND EE.dt >= '[:DateNoLine]'
                                AND EE.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',2),'yyyyMMdd'))
                        )
                ) DDD
            ) DDDD ON 1 = 1
                AND lower(CCCC.mainaccount) = lower(DDDD.mainaccountid)
            WHERE 1 = 1
            GROUP BY
                AAAA.accountid ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def __insert11002DataSQL_old_20191201_to_20201201(self, makeInfo):
        orderInsertSQLCode1 = """
                -- Make [:GameName] ME 11002
                INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION( game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11002')
                SELECT
                    MAX(lower(AAAA.email)) as CommonData_1 --as accountid
                    , AAAA.accountid as CommonData_2
                    , null as CommonData_3
                    , null as CommonData_4
                    , MAX(lower(CCCC.mainaccount)) as CommonData_5
                    , MAX(lower(DDDD.globalsn)) as CommonData_6
                    , null as CommonData_7
                    , null as CommonData_8
                    , null as CommonData_9
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
                    , null as UniqueStr_1 --as newbeanfunbindaccount
                    , null as UniqueStr_2 --as mainaccountid
                    , null as UniqueStr_3 --as serviceaccountid
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
                    , MAX(BBBB.registerdate) as UniqueTime_1
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
                FROM (
                    SELECT
                        AA.accountid
                        , lower(AA.email) as email
                    FROM gtwpd.maple_middle_login_record AA
                    WHERE 1 = 1
                        AND AA.dt = '[:DateNoLine]'
                ) AAAA
                INNER JOIN (
                    SELECT
                        BB.accountid
                        , BB.registerdate
                    FROM maple_extract.globalaccount_account_old_20201202 BB
                    WHERE 1 = 1
                        AND BB.dt = '[:DateNoLine]'
                ) BBBB ON 1 = 1
                    AND AAAA.accountid = BBBB.accountid
                LEFT JOIN (
                    SELECT
                        DISTINCT CCC.serviceaccountid
                        , CCC.mainaccount
                    FROM (
                        SELECT
                            CC.serviceaccountid
                            , FIRST_VALUE(CC.mainaccount) over (partition by CC.serviceaccountid order by CC.dt desc ) as mainaccount
                        FROM bf_extract.beanfundb_d_serviceaccount_f CC
                        WHERE 1 = 1
                            AND not ( not 1 != 1
                                and not ('[:DateNoLine]' < '20200701' AND CC.dt = '20200701')
                                and not ( 1 = 1
                                    AND '[:DateNoLine]' >= '20200701'
                                    AND CC.dt >= '[:DateNoLine]'
                                    AND CC.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',2),'yyyyMMdd'))
                            )
                            AND CC.servicecode = 610074
                    ) CCC
                ) CCCC ON 1 = 1
                    AND lower(AAAA.email) = lower(CCCC.serviceaccountid)
                LEFT JOIN (
                    SELECT
                        DISTINCT DDD.mainaccountid
                        , DDD.globalsn
                    FROM (
                        SELECT
                            DD.mainaccountid
                            , FIRST_VALUE(DD.globalsn) over (partition by DD.mainaccountid order by DD.dt desc ) as globalsn
                        FROM bf_extract.beanfundb_bfapp_mainaccount DD
                        WHERE 1 = 1
                            AND not ( not 1 != 1
                                and not ('[:DateNoLine]' < '20200701' AND DD.dt = '20200701')
                                and not ( 1 = 1
                                    AND '[:DateNoLine]' >= '20200701'
                                    AND DD.dt >= '[:DateNoLine]'
                                    AND DD.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',2),'yyyyMMdd'))
                            )
                    ) DDD
                ) DDDD ON 1 = 1
                    AND lower(CCCC.mainaccount) = lower(DDDD.mainaccountid)
                WHERE 1 = 1
                GROUP BY
                    AAAA.accountid ;
            """
        return "OrderInsert", [orderInsertSQLCode1]