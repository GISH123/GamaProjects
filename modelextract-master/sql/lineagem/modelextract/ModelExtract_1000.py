import time


class ModelExtract_1000 :

    # layer_01
    @classmethod
    def insert1001DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1001 ModelExtract Data
            FROM ( 
                SELECT a.CommonData_1,c.externalaccountkey AS CommonData_6,c.authprovidercode AS CommonData_10
                ,a.UniqueStr_2 
                ,SUM(CASE WHEN a.UniqueTime_1 < '[:DateLine]' THEN 0 ELSE 1 END) AS login_cnt
                ,SUM(CASE WHEN a.UniqueTime_2 >= DATE_ADD('[:DateLine]',1) THEN 0 ELSE 1 END) AS logout_cnt
                FROM rcenter.gama_bda_model_extract a 
                LEFT OUTER JOIN np.np_gameaccounts b
                ON a.CommonData_1 = b.gameaccountid 
                LEFT OUTER JOIN np.np_externalassociations c
                ON b.userid = c.userid
                WHERE a.game = '[:GameName]'
                AND   a.dt = '[:DateNoLine]'
                AND   a.tablenumber = '11005'
                GROUP BY a.CommonData_1,a.UniqueStr_2,c.externalaccountkey ,c.authprovidercode
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1001')
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, CommonData_6 AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , login_cnt AS UniqueInt_1, logout_cnt AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , NULL AS UniqueStr_1, NULL AS UniqueStr_2, NULL AS UniqueStr_3
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
            , UniqueStr_2 AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert1002DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1002 ModelExtract Data
            FROM ( 
                SELECT a.CommonData_1,c.externalaccountkey AS CommonData_6,c.authprovidercode AS CommonData_10
                ,a.UniqueStr_2,a.world
                ,SUM(CASE WHEN a.UniqueTime_1 < '[:DateLine]' THEN 0 ELSE 1 END) AS login_cnt
                ,SUM(CASE WHEN a.UniqueTime_2 >= DATE_ADD('[:DateLine]',1) THEN 0 ELSE 1 END) AS logout_cnt
                FROM rcenter.gama_bda_model_extract a
                LEFT OUTER JOIN np.np_gameaccounts b
                ON a.CommonData_1 = b.gameaccountid 
                LEFT OUTER JOIN np.np_externalassociations c
                ON b.userid = c.userid
                WHERE a.game = '[:GameName]'
                AND   a.dt = '[:DateNoLine]'
                AND   a.tablenumber = '11005'
                GROUP BY a.CommonData_1,a.UniqueStr_2,a.world,c.externalaccountkey,c.authprovidercode
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1002')
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, CommonData_6 AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , login_cnt AS UniqueInt_1, logout_cnt AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , NULL AS UniqueStr_1, NULL AS UniqueStr_2, NULL AS UniqueStr_3
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
            , UniqueStr_2 AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert1003DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1003 ModelExtract Data
            FROM ( 
                SELECT a.CommonData_1,a.CommonData_3,a.CommonData_4
                ,c.externalaccountkey AS CommonData_6,c.authprovidercode AS CommonData_10
                ,SUM(CASE WHEN a.UniqueTime_1 < '[:DateLine]' THEN 0 ELSE 1 END) AS login_cnt
                ,SUM(CASE WHEN a.UniqueTime_2 >= DATE_ADD('[:DateLine]',1) THEN 0 ELSE 1 END) AS logout_cnt
                ,a.UniqueStr_3,a.world
                FROM rcenter.gama_bda_model_extract a
                LEFT OUTER JOIN np.np_gameaccounts b
                ON a.CommonData_1 = b.gameaccountid 
                LEFT OUTER JOIN np.np_externalassociations c
                ON b.userid = c.userid
                WHERE a.game = '[:GameName]'
                AND   a.dt = '[:DateNoLine]'
                AND   a.tablenumber = '11005'
                GROUP BY a.CommonData_1,a.CommonData_3,a.CommonData_4
                ,c.externalaccountkey ,c.authprovidercode,UniqueStr_3,world
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1003') 
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, CommonData_3 AS CommonData_3
            , CommonData_4 AS CommonData_4, NULL AS CommonData_5, CommonData_6 AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , login_cnt AS UniqueInt_1, logout_cnt AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , NULL AS UniqueStr_1, NULL AS UniqueStr_2, NULL AS UniqueStr_3
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
            , UniqueStr_3 AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1
            WHERE world = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]
