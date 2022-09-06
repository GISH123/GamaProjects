import time


class ModelExtract_1800 :

    # layer_02
    @classmethod
    def insert1804DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1804 ModelExtract Data
            FROM(
                SELECT LOWER(AA.CommonData_1) AS CommonData_1,BB.CommonData_5,BB.CommonData_6
                ,NVL(BB.CommonData_7,'HK') AS CommonData_7,BB.CommonData_10
                ,AA.UniqueInt_1 AS UniqueInt_2,AA.UniqueInt_3,AA.UniqueInt_5 AS UniqueInt_1,AA.UniqueStr_1
                ,COLLECT_SET(CC.CommonData_1) AS ACC_Arr,NVL(BB.istest,0) AS istest
                FROM gtwpd.modelextract_modelextract AA
                LEFT OUTER JOIN (
                    SELECT CommonData_1,CommonData_5,CommonData_6,CommonData_7,CommonData_10,CommonData_11 AS istest
                    FROM gtwpd.modelextract_modelextract
                    WHERE game = '[:GameName]'
                    AND   dt = '[:DateNoLine]'
                    AND   world = 'COMMON'
                    AND   tablenumber = '10001'
                ) BB
                ON LOWER(AA.CommonData_1) = BB.CommonData_1
                INNER JOIN gtwpd.modelextract_modelextract CC
                ON AA.UniqueStr_1 = CC.UniqueStr_1 AND CC.tablenumber = '11002' AND CC.dt = '[:DateNoLine]'
                WHERE AA.game = '[:GameName]'
                AND   AA.dt = '[:DateNoLine]'
                AND   AA.tablenumber = '11002'
                GROUP BY LOWER(AA.CommonData_1),BB.CommonData_5,BB.CommonData_6,NVL(BB.CommonData_7,'HK')
                ,BB.CommonData_10,AA.UniqueInt_1,AA.UniqueInt_3,AA.UniqueInt_5,AA.UniqueStr_1,NVL(BB.istest,0)
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1804')
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , UniqueStr_1 AS UniqueStr_1, NULL AS UniqueStr_2, NULL AS UniqueStr_3
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
            , ACC_Arr AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE istest = 0 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    # layer_03
    @classmethod
    def insert1802DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1802 ModelExtract Data
            FROM(
                SELECT DISTINCT LOWER(AA.CommonData_1) AS CommonData_1,BB.CommonData_5,BB.CommonData_6
                ,NVL(BB.CommonData_7,'HK') AS CommonData_7,BB.CommonData_10
                ,AA.UniqueInt_1 AS UniqueInt_2,AA.UniqueInt_5 AS UniqueInt_1,AA.UniqueStr_1
                ,CC.UniqueStr_2,CC.UniqueStr_3,CC.UniqueStr_4,CC.UniqueStr_5,CC.UniqueStr_6
                ,CC.UniqueStr_7,CC.uniquedbl_1,CC.uniquedbl_2 
                FROM gtwpd.modelextract_modelextract AA
                LEFT OUTER JOIN (
                    SELECT CommonData_1,CommonData_5,CommonData_6,CommonData_7,CommonData_10,CommonData_11 AS istest
                    FROM gtwpd.modelextract_modelextract
                    WHERE game = '[:GameName]'
                    AND   dt = '[:DateNoLine]'
                    AND   world = 'COMMON'
                    AND   tablenumber = '10001'
                ) BB
                ON LOWER(AA.CommonData_1) = BB.CommonData_1
                LEFT OUTER JOIN gtwpd.modelextract_modelextract CC
                ON AA.UniqueInt_5 = CC.UniqueInt_5 AND CC.tablenumber = '11802' AND CC.dt = '[:DateNoLine]'
                WHERE AA.game = '[:GameName]'
                AND   AA.dt = '[:DateNoLine]'
                AND   AA.tablenumber = '11002'
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1802')
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , UniqueStr_1 AS UniqueStr_1, UniqueStr_2 AS UniqueStr_2, UniqueStr_3 AS UniqueStr_3
            , UniqueStr_4 AS UniqueStr_4, UniqueStr_5 AS UniqueStr_5, UniqueStr_6 AS UniqueStr_6
            , UniqueStr_7 AS UniqueStr_7, NULL AS UniqueStr_8, NULL AS UniqueStr_9
            , NULL AS UniqueStr_10, NULL AS UniqueStr_11, NULL AS UniqueStr_12
            , NULL AS UniqueStr_13, NULL AS UniqueStr_14, NULL AS UniqueStr_15
            , NULL AS UniqueStr_16, NULL AS UniqueStr_17, NULL AS UniqueStr_18
            , NULL AS UniqueStr_19, NULL AS UniqueStr_20
            , UniqueDbl_1 AS UniqueDbl_1, UniqueDbl_2 AS UniqueDbl_2, NULL AS UniqueDbl_3
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
            WHERE istest = 0 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert1806DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1806 ModelExtract Data
            FROM(
                SELECT DISTINCT AA.CommonData_1,BB.CommonData_5,BB.CommonData_6,BB.CommonData_7,BB.CommonData_10
                ,AA.UniqueInt_1 AS UniqueInt_2,AA.UniqueInt_5 AS UniqueInt_1,AA.UniqueStr_1
                ,CC.UniqueStr_2,CC.UniqueStr_3,CC.UniqueStr_4,CC.UniqueStr_5,CC.UniqueStr_6
                ,CC.UniqueStr_7,CC.UniqueStr_8,CC.UniqueStr_11,CC.UniqueStr_12,CC.UniqueStr_13
                ,CC.uniquedbl_1,CC.uniquedbl_2,BB.istest
                FROM gtwpd.modelextract_modelextract AA
                LEFT OUTER JOIN (
                    SELECT CommonData_1,CommonData_5,CommonData_6,CommonData_7,CommonData_10,CommonData_11 AS istest
                    FROM gtwpd.modelextract_modelextract
                    WHERE game = '[:GameName]'
                    AND   dt = '[:DateNoLine]'
                    AND   world = 'COMMON'
                    AND   tablenumber = '10001'
                ) BB
                ON LOWER(AA.CommonData_1) = BB.CommonData_1
                LEFT OUTER JOIN gtwpd.modelextract_modelextract CC
                ON AA.UniqueInt_5 = CC.UniqueInt_5 AND CC.tablenumber = '11806' AND CC.dt = '[:DateNoLine]'
                WHERE AA.game = '[:GameName]'
                AND   AA.dt = '[:DateNoLine]'
                AND   AA.tablenumber = '11002'
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1806')
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , UniqueStr_1 AS UniqueStr_1, UniqueStr_2 AS UniqueStr_2, UniqueStr_3 AS UniqueStr_3
            , UniqueStr_4 AS UniqueStr_4, UniqueStr_5 AS UniqueStr_5, UniqueStr_6 AS UniqueStr_6
            , UniqueStr_7 AS UniqueStr_7, UniqueStr_8 AS UniqueStr_8, NULL AS UniqueStr_9
            , NULL AS UniqueStr_10, UniqueStr_11 AS UniqueStr_11, UniqueStr_12 AS UniqueStr_12
            , UniqueStr_13 AS UniqueStr_13, NULL AS UniqueStr_14, NULL AS UniqueStr_15
            , NULL AS UniqueStr_16, NULL AS UniqueStr_17, NULL AS UniqueStr_18
            , NULL AS UniqueStr_19, NULL AS UniqueStr_20
            , UniqueDbl_1 AS UniqueDbl_1, UniqueDbl_2 AS UniqueDbl_2, NULL AS UniqueDbl_3
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
            WHERE istest = 0 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]