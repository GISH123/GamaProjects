import time
from sql.common.modelextract.ModelExtract_1100 import ModelExtract_1100 as ModelExtract_1100_Common


class ModelExtract_1100(ModelExtract_1100_Common):

    # layer_03
    @classmethod
    def insert1102DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1102 ModelExtract Data
            FROM(
                SELECT LOWER(AA.CommonData_1) AS CommonData_1,BB.CommonData_5,BB.CommonData_6
                ,NVL(BB.CommonData_7,'HK') AS CommonData_7,BB.CommonData_10
                ,AA.UniqueTime_1,AA.UniqueTime_2,AA.world,NVL(BB.istest,0) AS istest
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
                WHERE game = '[:GameName]'
                AND   dt = '[:DateNoLine]'
                AND   tablenumber = '11103'
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1102')
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , NULL AS UniqueInt_1, NULL AS UniqueInt_2, NULL AS UniqueInt_3
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
            , UniqueTime_1 AS UniqueTime_1, UniqueTime_2 AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world = '[:World]' 
            AND   istest = 0 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert1103DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1103 ModelExtract Data
            FROM(
                SELECT LOWER(AA.CommonData_1) AS CommonData_1,AA.CommonData_3,AA.CommonData_4
                ,BB.CommonData_5,BB.CommonData_6,NVL(BB.CommonData_7,'HK') AS CommonData_7,BB.CommonData_10
                ,AA.UniqueTime_1,AA.UniqueTime_2,AA.world,NVL(BB.istest,0) AS istest
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
                WHERE game = '[:GameName]'
                AND   dt = '[:DateNoLine]'
                AND   tablenumber = '11103'
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1103')
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, CommonData_3 AS CommonData_3
            , CommonData_4 AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , NULL AS UniqueInt_1, NULL AS UniqueInt_2, NULL AS UniqueInt_3
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
            , UniqueTime_1 AS UniqueTime_1, UniqueTime_2 AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world = '[:World]' 
            AND   istest = 0 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    # layer_04
    '''@classmethod
    def insert1131DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1131 ModelExtract Data
            FROM ( 
                SELECT *
                FROM gtwpd.modelextract_modelextract AA
                    WHERE AA.game = '[:GameName]'
                    AND   AA.dt = '[:DateNoLine]'
                    AND   AA.tablenumber = '11131'
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1131')
            SELECT NULL AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
            , CommonData_7 AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , sum(UniqueInt_1) OVER (PARTITION BY CommonData_7 ORDER BY UniqueTime_1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS UniqueInt_1, NULL AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , NULL AS UniqueStr_1, NULL AS UniqueStr_2, NULL AS UniqueStr_3
            , NULL AS UniqueStr_4, NULL AS UniqueStr_5, NULL AS UniqueStr_6
            , NULL AS UniqueStr_7, NULL AS UniqueStr_8, NULL AS UniqueStr_9
            , NULL AS UniqueStr_10, UniqueTime_1 AS UniqueTime_1, NULL AS UniqueTime_2
            , NULL AS UniqueTime_3, ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world in ('COMMONM','[:World]') """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]'''

    @classmethod
    def insert1131DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] 1131 ModelExtract Data
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1131')
            SELECT NULL AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
            , AA.CommonData_7 AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , sum(AA.UniqueInt_1) OVER (PARTITION BY AA.CommonData_7 ORDER BY AA.UniqueTime_1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS UniqueInt_1, NULL AS UniqueInt_2, NULL AS UniqueInt_3
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
            , AA.UniqueTime_1 AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            FROM gtwpd.modelextract_modelextract AA
            WHERE AA.game = '[:GameName]'
            AND   AA.dt = '[:DateNoLine]'
            AND   AA.tablenumber = '11131' 
            AND   AA.world in ('COMMONM','[:World]');
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1132DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1132 ModelExtract Data
            FROM(
                SELECT AA.CommonData_1, AA.CommonData_3, AA.CommonData_4
                , AA.CommonData_5, AA.CommonData_6, AA.CommonData_7
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*0)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*0)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*0)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*0
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*0)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*0
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_1   
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*1)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*1)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*1)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*1
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*1)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*1
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_2     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*2)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*2)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*2)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*2
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*2)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*2
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_3     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*3)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*3)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*3)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*3
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*3)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*3
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_4     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*4)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*4)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*4)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*4
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*4)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*4
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_5     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*5)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*5)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*5)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*5
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*5)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*5
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_6     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*6)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*6)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*6)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*6
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*6)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*6
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_7     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*7)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*7)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*7)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*7
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*7)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*7
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_8     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*8)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*8)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*8)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*8
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*8)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*8
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_9    
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*9)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*9)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*9)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*9
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*9)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*9
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_10    
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*10)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*10)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*10)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*10
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*10)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*10
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_11     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*11)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*11)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*11)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*11
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*11)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*11
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_12
                , AA.world
                FROM gtwpd.modelextract_modelextract AA
                WHERE AA.game = '[:GameName]'
                AND   AA.dt = '[:DateNoLine]'
                AND   AA.tablenumber = '1103'
                GROUP BY AA.CommonData_1, AA.CommonData_3, AA.CommonData_4
                , AA.CommonData_5, AA.CommonData_6, AA.CommonData_7, AA.world
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1132')
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, CommonData_3 AS CommonData_3
            , CommonData_4 AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , UniqueInt_4 AS UniqueInt_4, UniqueInt_5 AS UniqueInt_5, UniqueInt_6 AS UniqueInt_6
            , UniqueInt_7 AS UniqueInt_7, UniqueInt_8 AS UniqueInt_8, UniqueInt_9 AS UniqueInt_9
            , UniqueInt_10 AS UniqueInt_10, UniqueInt_11 AS UniqueInt_11, UniqueInt_12 AS UniqueInt_12
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
            , NULL AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert1133DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1133 ModelExtract Data
            FROM(
                SELECT AA.CommonData_1, AA.CommonData_3, AA.CommonData_4
                , AA.CommonData_5, AA.CommonData_6, AA.CommonData_7
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*12)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*12)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*12)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*12
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*12)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*12
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_1     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*13)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*13)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*13)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*13
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*13)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*13
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_2     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*14)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*14)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*14)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*14
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*14)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*14
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_3    
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*15)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*15)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*15)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*15
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*15)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*15
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_4     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*16)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*16)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*16)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*16
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*16)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*16
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_5     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*17)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*17)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*17)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*17
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*17)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*17
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_6     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*18)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*18)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*18)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*18
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*18)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*18
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_7    
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*19)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*19)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*19)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*19
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*19)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*19
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_8    
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*20)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*20)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*20)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*20
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*20)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*20
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_9     
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*21)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*21)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*21)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*21
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*21)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*21
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_10
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*22)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*22)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*22)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*22
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*22)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*22
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_11 
                , SUM(CASE 
                    WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*23)
                        AND AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*23)
                        THEN  
                            CASE
                                WHEN AA.uniquetime_2 >= from_unixtime(unix_timestamp('[:DateLine] 00:59:59')+3600*23)
                                    THEN unix_timestamp('[:DateLine] 00:59:59')+3600*23
                                ELSE unix_timestamp(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                            END -
                            CASE 
                                WHEN AA.uniquetime_1 <= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*23)
                                    THEN unix_timestamp('[:DateLine] 00:00:00')+3600*23
                                ELSE unix_timestamp(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                            END + 1 
                    ELSE 0 
                END	)as UniqueInt_12    
                , count(uniquetime_1) as UniqueInt_14
                , SUM(unix_timestamp(uniquetime_2) - unix_timestamp(uniquetime_1)) as UniqueInt_15
                , AA.world
                FROM gtwpd.modelextract_modelextract AA
                WHERE AA.game = '[:GameName]'
                AND   AA.dt = '[:DateNoLine]'
                AND   AA.tablenumber = '1103'
                GROUP BY AA.CommonData_1, AA.CommonData_3, AA.CommonData_4
                , AA.CommonData_5, AA.CommonData_6, AA.CommonData_7, AA.world
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1133')
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, CommonData_3 AS CommonData_3
            , CommonData_4 AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , UniqueInt_4 AS UniqueInt_4, UniqueInt_5 AS UniqueInt_5, UniqueInt_6 AS UniqueInt_6
            , UniqueInt_7 AS UniqueInt_7, UniqueInt_8 AS UniqueInt_8, UniqueInt_9 AS UniqueInt_9
            , UniqueInt_10 AS UniqueInt_10, UniqueInt_11 AS UniqueInt_11, UniqueInt_12 AS UniqueInt_12
            , NULL AS UniqueInt_13, UniqueInt_14 AS UniqueInt_14, UniqueInt_15 AS UniqueInt_15
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
            , NULL AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    # layer_05
    @classmethod
    def insert1134DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1134 ModelExtract Data
            FROM(
                SELECT AA.world
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*0) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*1) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_1
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*1) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*2) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_2
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*2) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*3) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_3
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*3) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*4) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_4
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*4) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*5) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_5
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*5) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*6) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_6
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*6) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*7) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_7
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*7) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*8) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_8
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*8) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*9) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_9
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*9) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*10) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_10
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*10) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*11) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_11
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*11) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*12) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_12
                FROM gtwpd.modelextract_modelextract AA
                WHERE AA.game = '[:GameName]'
                AND   AA.dt = '[:DateNoLine]'
                AND   AA.tablenumber = '1131'
                GROUP BY AA.world
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1134')
            SELECT NULL AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , UniqueInt_4 AS UniqueInt_4, UniqueInt_5 AS UniqueInt_5, UniqueInt_6 AS UniqueInt_6
            , UniqueInt_7 AS UniqueInt_7, UniqueInt_8 AS UniqueInt_8, UniqueInt_9 AS UniqueInt_9
            , UniqueInt_10 AS UniqueInt_10, UniqueInt_11 AS UniqueInt_11, UniqueInt_12 AS UniqueInt_12
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
            , NULL AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert1135DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1135 ModelExtract Data
            FROM(
                SELECT AA.world
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*12) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*13) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_1
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*13) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*14) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_2
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*14) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*18) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_3
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*15) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*16) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_4
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*16) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*17) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_5
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*17) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*18) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_6
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*18) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*19) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_7
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*19) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*20) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_8
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*20) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*21) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_9
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*21) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*22) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_10
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*22) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*23) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_11
                , MAX(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*23) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*24) THEN AA.UniqueInt_1 ELSE 0 END ) as UniqueInt_12
                FROM gtwpd.modelextract_modelextract AA
                WHERE AA.game = '[:GameName]'
                AND   AA.dt = '[:DateNoLine]'
                AND   AA.tablenumber = '1131'
                GROUP BY AA.world
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1135')
            SELECT NULL AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , UniqueInt_4 AS UniqueInt_4, UniqueInt_5 AS UniqueInt_5, UniqueInt_6 AS UniqueInt_6
            , UniqueInt_7 AS UniqueInt_7, UniqueInt_8 AS UniqueInt_8, UniqueInt_9 AS UniqueInt_9
            , UniqueInt_10 AS UniqueInt_10, UniqueInt_11 AS UniqueInt_11, UniqueInt_12 AS UniqueInt_12
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
            , NULL AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert1136DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1136 ModelExtract Data
            FROM(
                SELECT AA.world
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*0) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*1) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_1
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*1) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*2) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_2
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*2) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*3) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_3
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*3) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*4) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_4
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*4) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*5) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_5
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*5) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*6) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_6
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*6) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*7) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_7
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*7) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*8) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_8
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*8) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*9) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_9
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*9) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*10) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_10
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*10) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*11) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_11
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*11) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*12) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_12
                FROM gtwpd.modelextract_modelextract AA
                WHERE AA.game = '[:GameName]'
                AND   AA.dt = '[:DateNoLine]'
                AND   AA.tablenumber = '1131'
                GROUP BY AA.world
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1136')
            SELECT NULL AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , UniqueInt_4 AS UniqueInt_4, UniqueInt_5 AS UniqueInt_5, UniqueInt_6 AS UniqueInt_6
            , UniqueInt_7 AS UniqueInt_7, UniqueInt_8 AS UniqueInt_8, UniqueInt_9 AS UniqueInt_9
            , UniqueInt_10 AS UniqueInt_10, UniqueInt_11 AS UniqueInt_11, UniqueInt_12 AS UniqueInt_12
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
            , NULL AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert1137DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1137 ModelExtract Data
            FROM(
                SELECT AA.world
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*12) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*13) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_1
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*13) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*14) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_2
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*14) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*18) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_3
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*15) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*16) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_4
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*16) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*17) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_5
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*17) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*18) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_6
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*18) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*19) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_7
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*19) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*20) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_8
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*20) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*21) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_9
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*21) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*22) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_10
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*22) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*23) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_11
                , ROUND(SUM(CASE WHEN AA.UniqueTime_1 >= from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*23) AND AA.UniqueTime_1 < from_unixtime(unix_timestamp('[:DateLine] 00:00:00')+3600*24) THEN AA.UniqueInt_1 ELSE 0 END )/3600,0) as UniqueInt_12
                FROM gtwpd.modelextract_modelextract AA
                WHERE AA.game = '[:GameName]'
                AND   AA.dt = '[:DateNoLine]'
                AND   AA.tablenumber = '1131'
                GROUP BY AA.world
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1137')
            SELECT NULL AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , UniqueInt_4 AS UniqueInt_4, UniqueInt_5 AS UniqueInt_5, UniqueInt_6 AS UniqueInt_6
            , UniqueInt_7 AS UniqueInt_7, UniqueInt_8 AS UniqueInt_8, UniqueInt_9 AS UniqueInt_9
            , UniqueInt_10 AS UniqueInt_10, UniqueInt_11 AS UniqueInt_11, UniqueInt_12 AS UniqueInt_12
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
            , NULL AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]