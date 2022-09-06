import time


class ModelExtract_1800 :

    # layer_01
    @classmethod
    def insert1804DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1804 ModelExtract Data
            FROM(
                SELECT a.CommonData_1,c.externalaccountkey AS CommonData_6,c.authprovidercode AS CommonData_10
                ,a.UniqueInt_1 AS UniqueInt_2,a.UniqueInt_3,a.UniqueStr_1
                ,COLLECT_SET(d.CommonData_1) AS ACC_Arr 
                FROM rcenter.gama_bda_model_extract a
                LEFT OUTER JOIN np.np_gameaccounts b
                ON a.CommonData_1 = b.gameaccountid 
                LEFT OUTER JOIN np.np_externalassociations c
                ON b.userid = c.userid
                INNER JOIN rcenter.gama_bda_model_extract d
                ON a.UniqueStr_1 = d.UniqueStr_1 AND d.tablenumber = '11002' AND d.dt = '[:DateNoLine]'
                WHERE a.game = '[:GameName]'
                AND   a.dt = '[:DateNoLine]'
                AND   a.tablenumber = '11002'
                GROUP BY a.CommonData_1,a.UniqueInt_3,a.UniqueInt_1,a.UniqueStr_1,c.externalaccountkey,c.authprovidercode 
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1804')
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, CommonData_6 AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , NULL AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
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
            , ACC_Arr AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]
