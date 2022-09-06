import time


class ModelExtract_11000 :

    # layer_01
    @classmethod
    def insert11001DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11001 ModelExtract Data
            FROM (
                SELECT LOWER(AA.serviceaccountid) AS CommonData_1,BB.CommonData_2,LOWER(AA.mainaccountid) AS CommonData_5
                ,BB.CommonData_6,BB.CommonData_7,AA.servicecode AS CommonData_8,BB.CommonData_9
                ,BB.CommonData_10,AA.ipaddress,BB.UniqueStr_2,BB.UniqueStr_3,BB.UniqueStr_4,BB.istest
                ,MAX(AA.main_acc_in_cnt) AS main_acc_in_cnt,MAX(AA.game_acc_in_cnt) AS game_acc_in_cnt
                ,MAX(AA.main_acc_out_cnt) AS main_acc_out_cnt ,MAX(AA.game_acc_out_cnt) AS game_acc_out_cnt
                FROM (
                    SELECT servicecode,LOWER(mainaccountid) AS mainaccountid,LOWER(serviceaccountid) AS serviceaccountid
                    ,ipaddress,NULL AS main_acc_in_cnt,NULL AS game_acc_in_cnt
                    ,COUNT(ipaddress) OVER(PARTITION BY servicecode,LOWER(mainaccountid),ipaddress) AS main_acc_out_cnt
                    ,COUNT(ipaddress) OVER(PARTITION BY servicecode,LOWER(serviceaccountid),ipaddress) AS game_acc_out_cnt
                    FROM bf_extract.[:HistoryPlay]
                    WHERE dt = '[:DateNoLine]'
                    AND   substr(logouttime ,0,10) = '[:DateLine]'
                    UNION ALL
                    SELECT servicecode,LOWER(mainaccountid) AS mainaccountid,LOWER(serviceaccountid) AS serviceaccountid
                    ,ipaddress,COUNT(ipaddress) OVER(PARTITION BY servicecode,LOWER(mainaccountid),ipaddress) AS main_acc_in_cnt
                    ,COUNT(ipaddress) OVER(PARTITION BY servicecode,LOWER(serviceaccountid),ipaddress) AS game_acc_in_cnt
                    ,NULL AS main_acc_out_cnt,NULL AS game_acc_out_cnt 
                    FROM bf_extract.[:HistoryPlay]
                    WHERE dt >= '[:DateNoLine]'
                    AND   dt <= if(DATE_ADD('[:DateLine]',25) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',25),'yyyyMMdd')) --最長25日內才記錄當天登入時間
                    AND   substr(logintime ,0,10) = '[:DateLine]'
                ) AA
                LEFT OUTER JOIN (
                    SELECT CommonData_1,CommonData_2,CommonData_5,CommonData_6,CommonData_7,CommonData_8
                    ,CommonData_9,CommonData_10,CommonData_11 AS istest,UniqueStr_2,UniqueStr_3,UniqueStr_4
                    FROM gtwpd.modelextract_modelextract
                    WHERE game = '[:GameName]'
                    AND   dt = '[:DateNoLine]'
                    AND   world = 'COMMON'
                    AND   tablenumber = '10001'
                ) BB 
                ON AA.servicecode = BB.CommonData_8 AND LOWER(AA.mainaccountid) = LOWER(BB.CommonData_5) 
                AND LOWER(AA.serviceaccountid) = LOWER(BB.CommonData_1)
                GROUP BY LOWER(AA.serviceaccountid),LOWER(AA.mainaccountid)
                ,BB.CommonData_2,BB.CommonData_6,BB.CommonData_7,AA.servicecode,BB.CommonData_9
                ,BB.CommonData_10,AA.ipaddress,BB.UniqueStr_2,BB.UniqueStr_3,BB.UniqueStr_4,BB.istest
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11001') 
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, CommonData_8 AS CommonData_8, CommonData_9 AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , main_acc_in_cnt AS UniqueInt_1, game_acc_in_cnt AS UniqueInt_2, main_acc_out_cnt AS UniqueInt_3
            , game_acc_out_cnt AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , ipaddress AS UniqueStr_1, UniqueStr_2 AS UniqueStr_2, UniqueStr_3 AS UniqueStr_3
            , UniqueStr_4 AS UniqueStr_4, NULL AS UniqueStr_5, NULL AS UniqueStr_6
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
            WHERE istest = 0 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert11002DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11002 ModelExtract Data
            FROM (
                SELECT DISTINCT AAA.CommonData_1,AAA.CommonData_2,AAA.CommonData_5,AAA.CommonData_6,AAA.CommonData_7,AAA.CommonData_8
                ,AAA.CommonData_9,AAA.CommonData_10,AAA.ipaddress,AAA.main_acc_ip_cnt,AAA.game_acc_ip_cnt
                ,BBB.ip_main_acc_cnt,BBB.ip_game_acc_cnt,AAA.istest
                FROM (    
                    SELECT LOWER(AA.serviceaccountid) AS CommonData_1,BB.CommonData_2,LOWER(AA.mainaccountid) AS CommonData_5
                    ,BB.CommonData_6,BB.CommonData_7,AA.servicecode AS CommonData_8,BB.CommonData_9
                    ,BB.CommonData_10,AA.ipaddress,BB.istest
                    ,COUNT(AA.ipaddress) OVER(PARTITION BY BB.CommonData_9,LOWER(AA.mainaccountid),AA.ipaddress) AS main_acc_ip_cnt
                    ,COUNT(AA.ipaddress) OVER(PARTITION BY BB.CommonData_9,LOWER(AA.serviceaccountid),AA.ipaddress) AS game_acc_ip_cnt    
                    FROM bf_extract.[:HistoryPlay] AA    
                    LEFT OUTER JOIN (
                        SELECT CommonData_1,CommonData_2,CommonData_5,CommonData_6,CommonData_7,CommonData_8
                        ,CommonData_9,CommonData_10,CommonData_11 AS istest
                        FROM gtwpd.modelextract_modelextract
                        WHERE game = '[:GameName]'
                        AND   dt = '[:DateNoLine]'
                        AND   world = 'COMMON'
                        AND   tablenumber = '10001'
                    ) BB 
                    ON AA.servicecode = BB.CommonData_8 AND LOWER(AA.mainaccountid) = LOWER(BB.CommonData_5) 
                    AND LOWER(AA.serviceaccountid) = LOWER(BB.CommonData_1)
                    WHERE dt >= '[:DateNoLine]'
                    AND   dt <= if(DATE_ADD('[:DateLine]',25) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',25),'yyyyMMdd'))
                    AND   substr(logintime,0,10) = '[:DateLine]'
                ) AAA    
                LEFT OUTER JOIN (    
                    SELECT ipaddress,servicecode,COUNT(DISTINCT LOWER(mainaccountid)) AS ip_main_acc_cnt
                    ,COUNT(DISTINCT LOWER(serviceaccountid)) AS ip_game_acc_cnt    
                    FROM bf_extract.[:HistoryPlay]    
                    WHERE dt >= '[:DateNoLine]'
                    AND   dt <= if(DATE_ADD('[:DateLine]',25) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',25),'yyyyMMdd'))
                    AND   substr(logintime,0,10) = '[:DateLine]'
                    GROUP BY ipaddress ,servicecode
                ) BBB    
                ON AAA.ipaddress = BBB.ipaddress AND AAA.CommonData_8 = BBB.servicecode 
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11002') 
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, CommonData_8 AS CommonData_8, CommonData_9 AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , main_acc_ip_cnt AS UniqueInt_1, game_acc_ip_cnt AS UniqueInt_2, ip_main_acc_cnt AS UniqueInt_3
            , ip_game_acc_cnt AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , ipaddress AS UniqueStr_1, NULL AS UniqueStr_2, NULL AS UniqueStr_3
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
            WHERE istest = 0 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert11005DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11005 ModelExtract Data
            FROM (
                SELECT DISTINCT AAA.CommonData_1,AAA.CommonData_2,AAA.CommonData_5,AAA.CommonData_6,AAA.CommonData_7,AAA.CommonData_8
                ,AAA.CommonData_9,AAA.CommonData_10,AAA.ipaddress,AAA.UniqueStr_2,AAA.UniqueStr_3,AAA.UniqueStr_4
                ,AAA.diffday,AAA.playtime,AAA.logintime,AAA.logouttime,AAA.istest 
                FROM ( 
                    SELECT LOWER(AA.serviceaccountid) AS CommonData_1,BB.CommonData_2,LOWER(AA.mainaccountid) AS CommonData_5
                    ,BB.CommonData_6,BB.CommonData_7,AA.servicecode AS CommonData_8,BB.CommonData_9
                    ,BB.CommonData_10,AA.ipaddress,BB.UniqueStr_2,BB.UniqueStr_3,BB.UniqueStr_4
                    ,datediff(AA.logouttime ,AA.logintime ) AS diffday 
                    ,unix_timestamp(AA.logouttime,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(AA.logintime,'yyyy-MM-dd HH:mm:ss') AS playtime
                    ,AA.logintime,AA.logouttime,BB.istest
                    FROM bf_extract.[:HistoryPlay] AA    
                    LEFT OUTER JOIN (
                        SELECT CommonData_1,CommonData_2,CommonData_5,CommonData_6,CommonData_7,CommonData_8
                        ,CommonData_9,CommonData_10,CommonData_11 AS istest,UniqueStr_2,UniqueStr_3,UniqueStr_4
                        FROM gtwpd.modelextract_modelextract
                        WHERE game = '[:GameName]'
                        AND   dt = '[:DateNoLine]'
                        AND   world = 'COMMON'
                        AND   tablenumber = '10001'
                    ) BB 
                    ON AA.servicecode = BB.CommonData_8 AND LOWER(AA.mainaccountid) = LOWER(BB.CommonData_5) 
                    AND LOWER(AA.serviceaccountid) = LOWER(BB.CommonData_1)
                    WHERE AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-25),'yyyyMMdd') 
                    AND   AA.dt <= if(DATE_ADD('[:DateLine]',25) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',25),'yyyyMMdd'))
                    AND   AA.logouttime >= DATE_ADD('[:DateLine]',0) 
                    AND   AA.logintime < DATE_ADD('[:DateLine]',1) 
                ) AAA 
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11005') 
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, CommonData_8 AS CommonData_8, CommonData_9 AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , diffday AS UniqueInt_1, playtime AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , ipaddress AS UniqueStr_1, UniqueStr_2 AS UniqueStr_2, UniqueStr_3 AS UniqueStr_3
            , UniqueStr_4 AS UniqueStr_4, NULL AS UniqueStr_5, NULL AS UniqueStr_6
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
            , logintime AS UniqueTime_1, logouttime AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1
            WHERE istest = 0 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]


