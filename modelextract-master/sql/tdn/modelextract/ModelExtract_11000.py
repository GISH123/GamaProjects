import time

class ModelExtract_11000() :

    @classmethod
    def insert11001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 11001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION( game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11001')
            SELECT DISTINCT
                CC.accountname as CommonData_1
                , AA.accountid as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , lower(DD.mainaccount) as CommonData_5
                , lower(DD.globalsn) as CommonData_6
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
                , login_count as UniqueInt_2
                , null as UniqueInt_3
                , logout_count as UniqueInt_4
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
                , DD.UniqueStr_2 as UniqueStr_2
                , DD.UniqueStr_3 as UniqueStr_3
                , DD.UniqueStr_4 as UniqueStr_4
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
            FROM (
                SELECT a.dt, a.accountid, login_count, logout_count
                FROM (
                    SELECT 
                        DATE_FORMAT(logindate, 'yyyyMMdd') as dt
                        , accountid
                        , COUNT(distinct sessionid) as login_count
                    FROM tdn_extract.dnstaging_accountloginlogs
                    WHERE dt = '[:DateNoLine]'
                    GROUP BY DATE_FORMAT(logindate, 'yyyyMMdd'), accountid
                ) as a
                LEFT JOIN (
                    SELECT 
                        DATE_FORMAT(logoutdate, 'yyyyMMdd') as dt
                        , accountid
                        , COUNT(distinct sessionid) as logout_count
                    FROM tdn_extract.dnstaging_accountlogoutlogs
                    WHERE dt = '[:DateNoLine]'
                    GROUP BY DATE_FORMAT(logoutdate, 'yyyyMMdd'), accountid
                ) as b
                ON a.accountid = b.accountid
            ) AA
            LEFT JOIN (
                SELECT
                    accountid
                    , LOWER(accountname) as accountname
                    , registerdate
                FROM tdn_extract.dnmembership_accounts
                WHERE dt = '[:DateNoLine]'
            ) CC
            ON AA.accountid = CC.accountid
            LEFT JOIN (
                SELECT
                    commondata_1 as accountname
                    , commondata_5 as mainaccount
                    , commondata_6 as globalsn
                    , UniqueStr_2, UniqueStr_3, UniqueStr_4
                FROM gtwpd.modelextract_modelextract BB
                WHERE 1 = 1 
                    AND BB.game = 'gamania'
                    AND BB.tablenumber = '10001'
                    AND BB.commondata_8 = '[:ServiceCode]'
                 	AND BB.commondata_11 = '0'
                    AND BB.dt = '[:DateNoLine]'
            ) DD 
            ON CC.accountname = DD.accountname ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert11002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 11002
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION( game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11002')
            SELECT DISTINCT
                CC.accountname as CommonData_1
                , AA.accountid as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , lower(DD.mainaccount) as CommonData_5
                , lower(DD.globalsn) as CommonData_6
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
                , login_ip_count as UniqueInt_2
                , null as UniqueInt_3
                , logout_ip_count as UniqueInt_4
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
                , ip_address as UniqueStr_1
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
            FROM (
                SELECT a.dt, a.accountid, a.ip_address, login_ip_count, logout_ip_count
                FROM (
                    SELECT 
                        DATE_FORMAT(logindate, 'yyyyMMdd') as dt
                        , accountid
                        , CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                        , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                        , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                        , '.', '0') as ip_address
                        , COUNT(distinct sessionid) as login_ip_count
                    FROM tdn_extract.dnstaging_accountloginlogs
                    WHERE dt = '[:DateNoLine]'
                    GROUP BY DATE_FORMAT(logindate, 'yyyyMMdd'), accountid, CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                    , '.', '0')
                ) as a
                LEFT JOIN (
                    SELECT 
                        DATE_FORMAT(logoutdate, 'yyyyMMdd') as dt
                        , accountid
                        , CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                        , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                        , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                        , '.', '0') as ip_address
                        , COUNT(distinct sessionid) as logout_ip_count
                    FROM tdn_extract.dnstaging_accountlogoutlogs
                    WHERE dt = '[:DateNoLine]'
                    GROUP BY DATE_FORMAT(logoutdate, 'yyyyMMdd'), accountid, CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                    , '.', '0')
                ) as b
                ON a.accountid = b.accountid
            ) AA
            LEFT JOIN (
                SELECT
                    accountid
                    , LOWER(accountname) as accountname
                    , registerdate
                FROM tdn_extract.dnmembership_accounts
                WHERE dt = '[:DateNoLine]'
            ) CC
            ON AA.accountid = CC.accountid
            LEFT JOIN (
                SELECT
                    commondata_1 as accountname
                    , commondata_5 as mainaccount
                    , commondata_6 as globalsn
                FROM gtwpd.modelextract_modelextract BB
                WHERE 1 = 1 
                    AND BB.game = 'gamania'
                    AND BB.tablenumber = '10001'
                    AND BB.commondata_8 = '[:ServiceCode]'
                 	AND BB.commondata_11 = '0'
                    AND BB.dt = '[:DateNoLine]'
            ) DD 
            ON CC.accountname = DD.accountname ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert11005DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 11005
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION( game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11005')
            SELECT DISTINCT
                BB.accountname as CommonData_1
                , AA.accountid as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , lower(CC.mainaccount) as CommonData_5
                , lower(CC.globalsn) as CommonData_6
                , null as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , days_count as UniqueInt_1
                , seconds_sum as UniqueInt_2
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
                , ip_address as UniqueStr_1
                , UniqueStr_2 as UniqueStr_2
                , UniqueStr_3 as UniqueStr_3
                , UniqueStr_4 as UniqueStr_4
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
                , logindate as UniqueTime_1
                , logoutdate as UniqueTime_2
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
                    accountid
                    , (cast(logoutdate as double) - cast(logindate as double)) as seconds_sum
                    , (cast(logoutdate as double) - cast(logindate as double))/3600 as days_count
                    , logindate
                    , logoutdate
                    , CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                    , '.', '0') as ip_address
                    , dt
                FROM tdn_extract.dnstaging_accountlogoutlogs
                WHERE dt = '[:DateNoLine]'
            ) AA
            LEFT JOIN (
                SELECT
                    accountid
                    , LOWER(accountname) as accountname
                    , MAX(registerdate) as registerdate
                FROM tdn_extract.dnmembership_accounts
                WHERE dt = '[:DateNoLine]'
                GROUP BY accountid, LOWER(accountname)
            ) BB
            ON  AA.accountid = BB.accountid
            LEFT JOIN (
                SELECT
                    LOWER(commondata_1) as accountname
                    , commondata_5 as mainaccount
                    , commondata_6 as globalsn
                    , UniqueStr_2, UniqueStr_3, UniqueStr_4
                FROM gtwpd.modelextract_modelextract BB
                WHERE 1 = 1 
                    AND BB.game = 'gamania'
                    AND BB.tablenumber = '10001'
                    AND BB.commondata_8 = '[:ServiceCode]'
                 	AND BB.commondata_11 = '0'
                    AND BB.dt = '[:DateNoLine]'
            ) CC
            ON BB.accountname = CC.accountname ;
        """
        return "OrderInsert", [orderInsertSQLCode1]