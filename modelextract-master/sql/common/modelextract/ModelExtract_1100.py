import time


class ModelExtract_1100 :

    @classmethod
    def insert1101DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 1101 ModelExtract Data
            FROM(
                SELECT CommonData_1,CommonData_2,CommonData_5,CommonData_6,CommonData_7,CommonData_10
                ,CASE WHEN UniqueTime_1 < '[:DateLine]' THEN '[:DateLine] 00:00:00' ELSE CAST(UniqueTime_1 AS STRING) END AS login_time
                ,CASE WHEN UniqueTime_2 >= DATE_ADD('[:DateLine]',1) THEN '[:DateLine] 23:59:59' ELSE CAST(UniqueTime_2 AS STRING) END AS logout_time
                ,CommonData_8 AS service_code
                FROM gtwpd.modelextract_modelextract 
                WHERE game = 'gamania'
                AND   dt = '[:DateNoLine]'
                AND   world = 'COMMON'
                AND   tablenumber = '11005'
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:Game]',dt='[:DateNoLine]',world='COMMON',tablenumber='1101')
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, NULL AS CommonData_3
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
            , login_time AS UniqueTime_1, logout_time AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE service_code = '[:ServiceCode]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert1131DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1131
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1131')
            SELECT
                null as CommonData_1
                , null as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , AAAA.country as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , sum(AAAA.ind) OVER (PARTITION BY AAAA.country ORDER BY AAAA.logtime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as UniqueInt_1
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
                , AAAA.logtime as UniqueTime_1
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
                    AAA.logtime
                    , AAA.country
                    , sum(AAA.ind) AS ind
                FROM (
                    SELECT
                        date_format(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss') AS logtime
                        , AA.commondata_7 as country
                        , SUM(1) as ind
                    FROM gtwpd.modelextract_modelextract AA
                    WHERE 1 = 1
                        AND AA.game = '[:GameName]'
                        AND AA.dt = '[:DateNoLine]'
                        AND AA.world = '[:World]'
                        AND AA.tablenumber = '1103'
                    GROUP BY
                        date_format(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                        , AA.commondata_7
                    UNION ALL
                    SELECT
                        date_format(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss') AS logtime
                        , AA.commondata_7 as country
                        , SUM(-1) AS ind
                    FROM gtwpd.modelextract_modelextract AA
                    WHERE 1 = 1
                        AND AA.game = '[:GameName]'
                        AND AA.dt = '[:DateNoLine]'
                        AND AA.world = '[:World]'
                        AND AA.tablenumber = '1103'
                        AND date_format(AA.uniquetime_2,'HH:mm:ss') != '23:59:59'
                    GROUP BY
                        date_format(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                        , AA.commondata_7
                    UNION ALL
                    SELECT
                        concat('[:DateLine] ',uniquestr_1) AS logtime
                        , 'HK' as country
                        , 0 AS ind
                    FROM gtwpd.modelextract_modelextract AA
                    WHERE 1 = 1
                        AND AA.tablenumber = 90001
                    UNION ALL
                    SELECT
                        concat('[:DateLine] ',uniquestr_1) AS logtime
                        , 'TW' as country
                        , 0 AS ind
                    FROM gtwpd.modelextract_modelextract AA
                    WHERE 1 = 1
                        AND AA.tablenumber = 90001
                ) AAA
                GROUP BY
                    AAA.logtime
                    , AAA.country
            ) AAAA ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1132DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1132
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1132')
            SELECT
                AA.commondata_1
                , AA.commondata_2
                , AA.commondata_3
                , AA.commondata_4
                , AA.commondata_5
                , AA.commondata_6
                , AA.commondata_7
                , AA.CommonData_8 
                , AA.CommonData_9 
                , AA.CommonData_10 
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
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
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = '1103'
            GROUP BY
                AA.commondata_1
                , AA.commondata_2
                , AA.commondata_3
                , AA.commondata_4
                , AA.commondata_5
                , AA.commondata_6
                , AA.commondata_7
                , AA.CommonData_8 
                , AA.CommonData_9 
                , AA.CommonData_10  ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1133DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1133
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1133')
            SELECT
                AA.commondata_1
                , AA.commondata_2
                , AA.commondata_3
                , AA.commondata_4
                , AA.commondata_5
                , AA.commondata_6
                , AA.commondata_7
                , AA.CommonData_8 
                , AA.CommonData_9 
                , AA.CommonData_10 
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
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
                , null as UniqueInt_13
                , count(uniquetime_1) as UniqueInt_14
                , SUM(unix_timestamp(uniquetime_2) - unix_timestamp(uniquetime_1)) as UniqueInt_15
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
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = '1103'
            GROUP BY
                AA.commondata_1
                , AA.commondata_2
                , AA.commondata_3
                , AA.commondata_4
                , AA.commondata_5
                , AA.commondata_6
                , AA.commondata_7
                , AA.CommonData_8 
                , AA.CommonData_9 
                , AA.CommonData_10  ;           
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1134DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1134
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1134')
            SELECT
                null as CommonData_1
                , null as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , AA.CommonData_7 as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
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
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = '1131'
            GROUP BY
                AA.CommonData_7 ;            
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1135DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1135
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1135')
            SELECT
                null as CommonData_1
                , null as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , AA.CommonData_7 as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
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
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = '1131'
            GROUP BY
                AA.CommonData_7 ;        
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1136DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1136
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1136')
            SELECT
                null as CommonData_1
                , null as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , AA.CommonData_7 as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
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
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = '1131'
            GROUP BY
                AA.CommonData_7 ;      
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1137DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 1137
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1137')
            SELECT
                null as CommonData_1
                , null as CommonData_2
                , null as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , AA.CommonData_7 as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
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
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = '[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = '1131'
            GROUP BY
                AA.CommonData_7 ;            
        """
        return "OrderInsert", [orderInsertSQLCode1]