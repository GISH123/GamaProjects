import time

class ModelExtract_6000 :

    @classmethod
    def insert6002DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 6002 ModelExtract Data
            FROM ( 
                SELECT CommonData_1,CommonData_2,CommonData_5,CommonData_6,CommonData_7,CommonData_10
                ,UniqueStr_5,SUM(UniqueInt_1) AS pay_amt,SUM(1) AS pay_cnt,UniqueStr_2,CommonData_8 AS service_code
                FROM gtwpd.modelextract_modelextract a
                WHERE game = 'gamania'
                AND   dt = '[:DateNoLine]'
                AND   world = 'COMMON'
                AND   tablenumber = '16002'
                GROUP BY CommonData_1,CommonData_2,CommonData_5,CommonData_6,CommonData_7,CommonData_10
                ,UniqueStr_5,UniqueStr_2,CommonData_8
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:Game]',dt='[:DateNoLine]',world='COMMON',tablenumber='6002') 
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, UniqueStr_5 AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , pay_amt AS UniqueInt_1, pay_cnt AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , UniqueStr_2 AS UniqueStr_1, NULL AS UniqueStr_2, NULL AS UniqueStr_3
            , NULL AS UniqueStr_4, NULL AS UniqueStr_5, NULL AS UniqueStr_6
            , NULL AS UniqueStr_7, NULL AS UniqueStr_8, NULL AS UniqueStr_9
            , NULL AS UniqueStr_10, NULL AS UniqueStr_11, NULL AS UniqueStr_12
            , NULL AS UniqueStr_13, NULL AS UniqueStr_14, NULL AS UniqueStr_15
            , NULL AS UniqueStr_16, NULL AS UniqueStr_17, NULL AS UniqueStr_18
            , NULL AS UniqueStr_19, NULL AS UniqueStr_20
            , NULL AS UniqueDbl_1, NULL AS UniqueDbl_2, NULL AS UniqueDbl_3
            , NULL AS UniqueDbl_4, NULL AS UniqueDbl_5, NULL AS UniqueDbl_6
            , NULL AS UniqueDbl_7, NULL AS UniqueDbl_8, NULL AS UniqueDbl_9
            , NULL AS UniqueDbl_10, NULL AS UniqueDbl_11, NULL AS UniqueDbl_12
            , NULL AS UniqueDbl_13, NULL AS UniqueDbl_14, NULL AS UniqueDbl_15
            , NULL AS UniqueDbl_16, NULL AS UniqueDbl_17, NULL AS UniqueDbl_18
            , NULL AS UniqueDbl_19, NULL AS UniqueDbl_20
            , NULL AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE service_code = '[:ServiceCode]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert6006DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:Game]',dt='[:DateNoLine]',world='COMMON',tablenumber='6006') 
            SELECT  
                AA.CommonData_1 as CommonData_1
                , AA.CommonData_2 as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , AA.CommonData_5 as CommonData_5
                , AA.CommonData_6 as CommonData_6
                , AA.CommonData_7 as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , 1 as UniqueInt_1
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
            FROM
                gtwpd.modelextract_modelextract AA
            WHERE 1=1 
                AND AA.game = '[:Game]'
                AND AA.tablenumber = '6002'
                AND AA.dt = '[:DateNoLine]'
            GROUP BY 
                AA.CommonData_1 
                , AA.CommonData_2
                , AA.CommonData_5
                , AA.CommonData_6 
                , AA.CommonData_7  ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert6007DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:Game]',dt='[:DateNoLineFirstMonth]',world='COMMON',tablenumber='6007') 
            SELECT  
                AA.CommonData_1 as CommonData_1
                , AA.CommonData_2 as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , AA.CommonData_5 as CommonData_5
                , AA.CommonData_6 as CommonData_6
                , AA.CommonData_7 as CommonData_7
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
                , AA.UniqueStr_1 as UniqueStr_1
                , CASE 
                    WHEN SUM(UniqueInt_1) >= 50001 THEN 'LV.5'
                    WHEN SUM(UniqueInt_1) >= 30001 THEN 'LV.4'
                    WHEN SUM(UniqueInt_1) >= 10001 THEN 'LV.3'
                    WHEN SUM(UniqueInt_1) >= 301 THEN 'LV.2'
                    WHEN SUM(UniqueInt_1) >= 1 THEN 'LV.1'
                    ELSE 'LV.0'
                  END as UniqueStr_2
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
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM
                gtwpd.modelextract_modelextract AA
            WHERE 1=1 
                AND AA.game = '[:Game]'
                AND AA.tablenumber = '6002'
                AND AA.dt >= '[:DateNoLineFirstPrevMonth]'
                AND AA.dt < '[:DateNoLineFirstMonth]'
            GROUP BY 
                AA.CommonData_1 
                , AA.CommonData_2
                , AA.CommonData_5
                , AA.CommonData_6 
                , AA.CommonData_7 
                , AA.UniqueStr_1 ; 
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert6011DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
                INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6011')
                SELECT
                    AA.CommonData_1
                    , AA.CommonData_2
                    , AA.CommonData_3
                    , AA.CommonData_4
                    , AA.CommonData_5
                    , AA.CommonData_6
                    , AA.CommonData_7
                    , null as CommonData_8
                    , null as CommonData_9
                    , AA.CommonData_10
                    , null as CommonData_11
                    , null as CommonData_12
                    , null as CommonData_13
                    , null as CommonData_14
                    , null as CommonData_15
                    , null as UniqueInt_1
                    , count(AA.UniqueInt_2) as UniqueInt_2
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
                    , AA.UniqueStr_1 as UniqueStr_1
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
                    , SUM(AA.uniquedbl_1) as uniquedbl_1 
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
                    , array(null) as UniqueArray_1
                    , array(null) as UniqueArray_2
                    , null as UniqueJson_1
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = '[:GameName]'
                    AND AA.dt = '[:DateNoLine]'
                    AND AA.world = '[:World]'
                    AND AA.tablenumber = '6019'
                GROUP BY
                    AA.CommonData_1
                    , AA.CommonData_2
                    , AA.CommonData_3
                    , AA.CommonData_4
                    , AA.CommonData_5
                    , AA.CommonData_6
                    , AA.commondata_7 
                    , AA.commondata_10
                    , AA.UniqueStr_1 ;
            """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert6012DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
                INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6012')
                SELECT
                    null as CommonData_1
                    , null as CommonData_2
                    , null as CommonData_3
                    , null as CommonData_4
                    , null as CommonData_5
                    , null as CommonData_6
                    , AA.CommonData_7
                    , AA.CommonData_8
                    , AA.CommonData_9
                    , null as CommonData_10
                    , null as CommonData_11
                    , null as CommonData_12
                    , null as CommonData_13
                    , null as CommonData_14
                    , null as CommonData_15
                    , null as UniqueInt_1
                    , count(AA.UniqueInt_2) as UniqueInt_2
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
                    , AA.UniqueStr_1 as UniqueStr_1
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
                    , SUM(AA.uniquedbl_1) as uniquedbl_1 
                    , AA.uniquedbl_2 as uniquedbl_2 
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
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = '[:GameName]'
                    AND AA.dt = '[:DateNoLine]'
                    AND AA.world = '[:World]'
                    AND AA.tablenumber = '6019'
                GROUP BY
                    AA.commondata_7
                    , AA.CommonData_8
                    , AA.CommonData_9
                    , AA.UniqueStr_1
                    , AA.uniquedbl_2 ;
            """
        return "OrderInsert", [orderInsertSQLCode1]