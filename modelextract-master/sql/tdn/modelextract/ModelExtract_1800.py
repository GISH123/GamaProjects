import time

class ModelExtract_1800() :

    @classmethod
    def insert1802DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1802
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1802')
            SELECT
                CC.CommonData_1 as CommonData_1
                , CC.CommonData_2  as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
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
                , loginip as UniqueInt_1
                , loginip_count as UniqueInt_2
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
                , AA.ip_address as UniqueStr_1
                , uniquestr_3 as UniqueStr_2
                , uniquestr_4 as UniqueStr_3
                , uniquestr_5 as UniqueStr_4
                , uniquestr_6 as UniqueStr_5
                , uniquestr_7 as UniqueStr_6
                , uniquestr_8 as UniqueStr_7
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
                , uniquedbl_1 as uniquedbl_1 
                , uniquedbl_2 as uniquedbl_2 
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
                    CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                    , '.', '0') as ip_address
                    , loginip
                    , accountid
                    , dt
                    , COUNT(loginip) as loginip_count
                FROM tdn_extract.dnstaging_accountloginlogs
                WHERE dt = '[:DateNoLine]'
                GROUP BY CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                    , '.', '0')
                    , loginip
                    , dt
                    , accountid
            ) AA
            LEFT JOIN (
                SELECT
                    uniquestr_1 as ip_address
                    , uniquestr_3
                    , uniquestr_5
                    , uniquestr_6
                    , uniquestr_7
                    , uniquestr_8
                    , uniquestr_11, uniquestr_12, uniquestr_13
                    , uniquedbl_1, uniquedbl_2
                    , dt
                FROM gtwpd.modelextract_modelextract
                WHERE tablenumber = '11861'
            ) BB
            ON AA.ip_address = BB.ip_address
            LEFT JOIN (
                SELECT CommonData_1, CommonData_2, CommonData_5, CommonData_6, CommonData_7, dt
                FROM gtwpd.modelextract_modelextract
                WHERE tablenumber = '1002'
                AND dt = '[:DateNoLine]'
            ) CC
            ON CC.CommonData_2 = AA.accountid ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1804DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1804
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1804')
            SELECT
                CC.CommonData_1 as CommonData_1
                , CC.CommonData_2  as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
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
                , loginip as UniqueInt_1
                , loginip_count as UniqueInt_2
                , account_count as UniqueInt_3
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
                , AA.ip_address as UniqueStr_1
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
                , uniquedbl_1 as uniquedbl_1 
                , uniquedbl_2 as uniquedbl_2 
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
                    CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                    , '.', '0') as ip_address
                    , loginip
                    , dt
                    , accountid
                    , COUNT(loginip) as loginip_count
                    , COUNT(DISTINCT accountid) as account_count
                FROM tdn_extract.dnstaging_accountloginlogs
                WHERE dt = '[:DateNoLine]'
                GROUP BY CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                    , '.', '0')
                    , loginip
                    , dt
                    , accountid
            ) AA
            LEFT JOIN (
                SELECT
                    uniquestr_1 as ip_address
                    , uniquestr_3
                    , uniquestr_5
                    , uniquestr_6
                    , uniquestr_7
                    , uniquestr_8
                    , uniquestr_11, uniquestr_12, uniquestr_13
                    , uniquedbl_1, uniquedbl_2
                    , dt
                FROM gtwpd.modelextract_modelextract
                WHERE tablenumber = '11861'
            ) BB
            ON AA.ip_address = BB.ip_address
            LEFT JOIN (
                SELECT CommonData_1, CommonData_2, CommonData_5, CommonData_6, CommonData_7, dt
                FROM gtwpd.modelextract_modelextract
                WHERE tablenumber = '1002'
                AND dt = '[:DateNoLine]'
            ) CC
            ON AA.accountid = CC.CommonData_2 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1806DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1806
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1806')
            SELECT
                CC.CommonData_1 as CommonData_1
                , CC.CommonData_2  as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
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
                , loginip as UniqueInt_1
                , loginip_count as UniqueInt_2
                , account_count as UniqueInt_3
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
                , AA.ip_address as UniqueStr_1
                , BB.UniqueStr_2 as UniqueStr_2
                , BB.UniqueStr_3 as UniqueStr_3
                , BB.UniqueStr_4 as UniqueStr_4
                , BB.UniqueStr_5 as UniqueStr_5
                , BB.UniqueStr_6 as UniqueStr_6
                , BB.UniqueStr_7 as UniqueStr_7
                , BB.UniqueStr_8 as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , BB.uniquestr_11 as uniquestr_11 
                , BB.uniquestr_12 as uniquestr_12 
                , BB.uniquestr_13 as uniquestr_13 
                , null as uniquestr_14 
                , null as uniquestr_15 
                , null as uniquestr_16 
                , null as uniquestr_17 
                , null as uniquestr_18 
                , null as uniquestr_19 
                , null as uniquestr_20 
                , uniquedbl_1 as uniquedbl_1 
                , uniquedbl_2 as uniquedbl_2 
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
                    CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                    , '.', '0') as ip_address
                    , loginip
                    , dt
                    , accountid
                    , COUNT(loginip) as loginip_count
                    , COUNT(DISTINCT accountid) as account_count
                FROM tdn_extract.dnstaging_accountloginlogs
                WHERE dt = '[:DateNoLine]'
                GROUP BY CONCAT(CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/16777216) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/65536%256) AS STRING)
                    , '.', CAST(FLOOR(IF(loginip<0, loginip+4294967296, loginip)/256%256) AS STRING)
                    , '.', '0')
                    , loginip
                    , dt
                    , accountid
            ) AA
            LEFT JOIN (
                SELECT *
                FROM gtwpd.modelextract_modelextract
                WHERE tablenumber = '11861'
            ) BB
            ON AA.ip_address = BB.uniquestr_1
            LEFT JOIN (
                SELECT CommonData_1, CommonData_2, CommonData_5, CommonData_6, CommonData_7, dt
                FROM gtwpd.modelextract_modelextract
                WHERE tablenumber = '1002'
                AND dt = '[:DateNoLine]'
            ) CC
            ON AA.accountid = CC.CommonData_2 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]