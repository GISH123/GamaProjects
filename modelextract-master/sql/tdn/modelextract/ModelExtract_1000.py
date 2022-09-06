import time
from sql.common.modelextract.ModelExtract_1000 import ModelExtract_1000 as ModelExtract_1000_Common

class ModelExtract_1000(ModelExtract_1000_Common) :

    @classmethod
    def insert1002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1002
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1002')
            SELECT DISTINCT
                BB.CommonData_1 as CommonData_1
                , BB.CommonData_2 as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , BB.CommonData_5 as CommonData_5
                , BB.CommonData_6 as CommonData_6
                , CASE WHEN BB.CommonData_5 is not null THEN 'TW' ELSE 'HK' END as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , login_count as UniqueInt_1
                , logout_count as UniqueInt_2
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
                , CC.registerdate as UniqueTime_1
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
            	SELECT *
            	FROM gtwpd.modelextract_modelextract
	            WHERE game = '[:GameName]'
	                AND dt = '[:DateNoLine]'
	                AND tablenumber = 11002
            ) BB
            ON AA.accountid = BB.CommonData_2
            LEFT JOIN (
            	SELECT *
            	FROM tdn_extract.dnmembership_accounts
	            WHERE dt = '[:DateNoLine]'
            ) CC
            ON AA.accountid = CC.accountid ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1003DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1003
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1003')
            SELECT DISTINCT
                CC.CommonData_1 as CommonData_1
                , CC.CommonData_2  as CommonData_2
                , BB.characterid as CommonData_3
                , BB.charactername as CommonData_4
                , CC.CommonData_5 as CommonData_5
                , CC.CommonData_6 as CommonData_6
                , CASE WHEN CC.CommonData_5 is not null THEN 'TW' ELSE 'HK' END as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , AA.login_count as UniqueInt_1
                , AA.logout_count as UniqueInt_2
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
                , BB.createdate as UniqueTime_1
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
                SELECT a.dt, a.characterid, login_count, logout_count
                FROM (
                    SELECT 
                        DATE_FORMAT(logindate, 'yyyyMMdd') as dt
                        , characterid
                        , COUNT(*) as login_count
                    FROM tdn_extract.dnstaging_characterloginlogs
                    WHERE dt = '[:DateNoLine]'
                    GROUP BY DATE_FORMAT(logindate, 'yyyyMMdd'), characterid
                ) as a
                LEFT JOIN (
                    SELECT 
                        DATE_FORMAT(logoutdate, 'yyyyMMdd') as dt
                        , characterid
                        , COUNT(*) as logout_count
                    FROM tdn_extract.dnstaging_characterlogoutlogs
                    WHERE dt = '[:DateNoLine]'
                    GROUP BY DATE_FORMAT(logoutdate, 'yyyyMMdd'), characterid
                ) as b
                ON a.characterid = b.characterid
            ) AA
            INNER JOIN (
                SELECT *
                FROM tdn_extract.dnworld_characters
                WHERE dt = '[:DateNoLine]'
                    AND worldid = '[:World]'
            ) BB
            ON AA.characterid = BB.characterid
            LEFT JOIN (
            	SELECT *
            	FROM gtwpd.modelextract_modelextract
	            WHERE game = '[:GameName]'
	                AND dt = '[:DateNoLine]'
	                AND tablenumber = 11002
            ) CC
            ON BB.accountid = CC.CommonData_2 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]