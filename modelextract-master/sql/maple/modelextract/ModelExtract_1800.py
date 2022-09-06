import time

class ModelExtract_1800() :

    @classmethod
    def insert1802DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1802
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION (game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1802')
            SELECT /*+streamtable(BBB)*/
                AAA.commondata_1 as commondata_1
                , AAA.commondata_2 as commondata_2
                , null as CommonData_3 
                , null as CommonData_4 
                , AAA.commondata_5 as commondata_5
                , AAA.commondata_6 as commondata_6
                , AAA.CommonData_7 as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , AAA.uniqueint_1 as uniqueint_1
                , AAA.UniqueInt_2   as UniqueInt_2
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
                , AAA.UniqueStr_1
                , BBB.commondata_7 as UniqueStr_2
                , BBB.uniquestr_4 as UniqueStr_3
                , BBB.uniquestr_5 as UniqueStr_4
                , BBB.UniqueStr_6 as UniqueStr_5
                , BBB.UniqueStr_7 as UniqueStr_6
                , BBB.UniqueStr_8 as UniqueStr_7
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
                , BBB.uniquedbl_1 as uniquedbl_1 
                , BBB.uniquedbl_2 as uniquedbl_2 
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
            FROM gtwpd.modelextract_modelextract AAA
            INNER JOIN (
                SELECT * 
                FROM gtwpd.modelextract_modelextract BB
                WHERE 1 = 1 
                    AND BB.tablenumber = '11854'
                    AND BB.dt in (
                        SELECT max(dt) 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '11854'
                    )	
            ) BBB ON 1 = 1 
            WHERE 1 = 1
                AND AAA.game = '[:GameName]' 
                AND AAA.dt='[:DateNoLine]' 
                AND AAA.world='COMMON' 
                AND AAA.tablenumber='11802'
                AND AAA.uniqueint_1 >= BBB.uniqueint_1
                AND AAA.uniqueint_1 <= BBB.uniqueint_2 ; 
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1804DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1804
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1804')
            SELECT
                BB.CommonData_1 as CommonData_1 --服務帳號ID
                , AA.CommonData_2  as CommonData_2 --遊戲帳號ID
                , null as CommonData_3 --角色帳號ID
                , null as CommonData_4 --角色姓名ID
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
                , Max(AA.UniqueInt_1) as UniqueInt_1 --IP
                , SUM(AA.UniqueInt_2) as UniqueInt_2  
                , COUNT(DISTINCT CC.CommonData_2) as UniqueInt_3
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
                , AA.UniqueStr_1 as UniqueStr_1 --IP
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
                , collect_set(CC.CommonData_2) as UniqueArray_01
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.modelextract_modelextract AA
            INNER JOIN gtwpd.modelextract_modelextract BB ON 1 = 1
                AND BB.game = '[:GameName]'
                AND BB.dt = '[:DateNoLine]'
                AND BB.tablenumber = 11003
                AND AA.CommonData_2 = BB.CommonData_2
            INNER JOIN gtwpd.modelextract_modelextract CC ON 1 = 1
                AND CC.game = '[:GameName]'
                AND CC.dt = '[:DateNoLine]'
                AND CC.tablenumber = 11802
                AND AA.UniqueStr_1 = CC.UniqueStr_1
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.tablenumber = 11802
            GROUP BY
                BB.CommonData_1
                , BB.CommonData_5
                , BB.CommonData_6
                , AA.CommonData_2
                , AA.UniqueStr_1 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]



    @classmethod
    def insert1806DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1806
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION (game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1806')
            SELECT /*+streamtable(BBB)*/
                AAA.commondata_1 as commondata_1
                , AAA.commondata_2 as commondata_2
                , null as CommonData_3 
                , null as CommonData_4 
                , AAA.commondata_5 as commondata_5
                , AAA.commondata_6 as commondata_6
                , AAA.CommonData_7 as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , AAA.uniqueint_1 as uniqueint_1
                , AAA.UniqueInt_2   as UniqueInt_2
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
                , AAA.UniqueStr_1
                , BBB.commondata_7 as UniqueStr_2
                , BBB.UniqueStr_2 as UniqueStr_3
                , BBB.UniqueStr_4 as UniqueStr_4
                , BBB.UniqueStr_5 as UniqueStr_5
                , BBB.UniqueStr_6 as UniqueStr_6
                , BBB.UniqueStr_7 as UniqueStr_7
                , BBB.UniqueStr_8 as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , BBB.UniqueStr_11 as uniquestr_11 
                , BBB.UniqueStr_12 as uniquestr_12 
                , BBB.UniqueStr_13 as uniquestr_13 
                , null as uniquestr_14 
                , null as uniquestr_15 
                , null as uniquestr_16 
                , null as uniquestr_17 
                , null as uniquestr_18 
                , null as uniquestr_19 
                , null as uniquestr_20 
                , BBB.uniquedbl_1 as uniquedbl_1 
                , BBB.uniquedbl_2 as uniquedbl_2 
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
            FROM gtwpd.modelextract_modelextract AAA
            INNER JOIN (
                SELECT * 
                FROM gtwpd.modelextract_modelextract BB
                WHERE 1 = 1 
                    AND BB.tablenumber = '11861'
                    AND BB.dt in (
                        SELECT max(dt) 
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '11861'
                    )	
            ) BBB 
			ON 1 = 1 
                AND AAA.game = 'maple' 
                AND AAA.dt='[:DateNoLine]' 
                AND AAA.world='COMMON' 
                AND AAA.tablenumber='11802'
                AND split(AAA.uniquestr_1, '\\\\.')[0] = BBB.uniquestr_11
                AND split(AAA.uniquestr_1, '\\\\.')[1] = BBB.uniquestr_12
				AND split(AAA.uniquestr_1, '\\\\.')[2] = BBB.uniquestr_13
				; 
        """
        return "OrderInsert", [orderInsertSQLCode1]