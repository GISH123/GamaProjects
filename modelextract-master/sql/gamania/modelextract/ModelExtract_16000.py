import time


class ModelExtract_16000 :

    # layer_01
    @classmethod
    def insert16001DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 16001 ModelExtract Data
            FROM (
                SELECT AA.servicecode,CommonData_2,LOWER(AA.mainaccountid) AS CommonData_5,CC.CommonData_6,CC.CommonData_7
                ,CC.CommonData_10,AA.ipaddress,BB.depositmethod,BB.description,AA.amount,AA.deposittime,CC.istest 
                FROM bf_extract.[:HistoryDeposit] AA 
                LEFT OUTER JOIN (
                    SELECT DISTINCT depositmethod,description
                    FROM bf_extract.beanfundb_fk_depositmethod
                    WHERE dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd')
                    AND   dt <= if(DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd'))
                ) BB 
                ON AA.depositmethod = BB.depositmethod 
                LEFT OUTER JOIN (
                    SELECT DISTINCT CommonData_2,CommonData_5,CommonData_6,CommonData_7,CommonData_10,CommonData_11 AS istest
                    FROM gtwpd.modelextract_modelextract
                    WHERE game = '[:GameName]'
                    AND   dt = '[:DateNoLine]'
                    AND   world = 'COMMON'
                    AND   tablenumber = '10001'
                ) CC 
                ON LOWER(AA.mainaccountid) = LOWER(CC.CommonData_5) 
                WHERE AA.dt = '[:DateNoLine]'
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='16001')
            SELECT NULL AS CommonData_1, CommonData_2 AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, servicecode AS CommonData_8, NULL AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , NULL AS UniqueInt_1, NULL AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , ipaddress AS UniqueStr_1, NULL AS UniqueStr_2, depositmethod AS UniqueStr_3
            , description AS UniqueStr_4, NULL AS UniqueStr_5, NULL AS UniqueStr_6
            , NULL AS UniqueStr_7, NULL AS UniqueStr_8, NULL AS UniqueStr_9
            , NULL AS UniqueStr_10, NULL AS UniqueStr_11, NULL AS UniqueStr_12
            , NULL AS UniqueStr_13, NULL AS UniqueStr_14, NULL AS UniqueStr_15
            , NULL AS UniqueStr_16, NULL AS UniqueStr_17, NULL AS UniqueStr_18
            , NULL AS UniqueStr_19, NULL AS UniqueStr_20
            , amount AS UniqueDbl_1, NULL AS UniqueDbl_2, NULL AS UniqueDbl_3
            , NULL AS UniqueDbl_4, NULL AS UniqueDbl_5, NULL AS UniqueDbl_6
            , NULL AS UniqueDbl_7, NULL AS UniqueDbl_8, NULL AS UniqueDbl_9
            , NULL AS UniqueDbl_10, NULL AS UniqueDbl_11, NULL AS UniqueDbl_12
            , NULL AS UniqueDbl_13, NULL AS UniqueDbl_14, NULL AS UniqueDbl_15
            , NULL AS UniqueDbl_16, NULL AS UniqueDbl_17, NULL AS UniqueDbl_18
            , NULL AS UniqueDbl_19, NULL AS UniqueDbl_20
            , deposittime AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1
            WHERE istest = 0 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert16002DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 16002 ModelExtract Data
            FROM (
                SELECT LOWER(AA.serviceaccountid) AS CommonData_1
                , BB.CommonData_2 , LOWER(AA.mainaccountid) AS CommonData_5
                ,BB.CommonData_6,BB.CommonData_7,AA.servicecode AS CommonData_8,BB.CommonData_9
                ,BB.CommonData_10,AA.ipaddress,AA.chargerule
                ,CASE WHEN AA.chargerule = 'ZZ' THEN '點數沖銷'
                     WHEN AA.chargerule = '0W' THEN '遊戲扣點'
                     WHEN AA.chargerule = '0N' THEN '自由點數'
                     WHEN AA.chargerule = '00' THEN '天堂月服12時'
                     WHEN AA.chargerule = '02' THEN '天堂月服2時'
                     WHEN AA.chargerule = '05' THEN '天堂月服31天'
                     ELSE AA.chargerule END  AS chargerule_name
                ,AA.chargepoints,AA.chargeservicepoints,AA.chargepoints + AA.chargeservicepoints AS totalpoints
                ,AA.memo,AA.logouttime,BB.istest
                FROM bf_extract.[:HistoryPlay] AA 
                LEFT OUTER JOIN (
                    SELECT CommonData_1 , CommonData_2,CommonData_5,CommonData_6,CommonData_7,CommonData_8
                    ,CommonData_9,CommonData_10,CommonData_11 AS istest
                    FROM gtwpd.modelextract_modelextract
                    WHERE game = '[:GameName]'
                    AND   dt = '[:DateNoLine]'
                    AND   world = 'COMMON'
                    AND   tablenumber = '10001'
                ) BB 
                ON AA.servicecode = BB.CommonData_8 AND LOWER(AA.mainaccountid) = LOWER(BB.CommonData_5) 
                AND LOWER(AA.serviceaccountid) = LOWER(BB.CommonData_1)
                WHERE dt = '[:DateNoLine]'
                AND   SUBSTR(logouttime ,0,10) = '[:DateLine]'
                AND   AA.servicecode NOT IN ('999995','999973')
                AND   (AA.chargepoints != 0 OR chargeservicepoints != 0)
                UNION ALL 
                SELECT LOWER(AA.serviceaccountid) AS CommonData_1, BB.CommonData_2,LOWER(AA.mainaccountid) AS CommonData_5
                ,BB.CommonData_6,BB.CommonData_7,AA.servicecode AS CommonData_8
                ,CASE WHEN servicecode = '999995' THEN "天堂專用線上交易服務"
                      WHEN servicecode = '999973' THEN "MapleStory Event 線上交易" ELSE servicecode END AS CommonData_9
                ,BB.CommonData_10,AA.ipaddress,AA.chargerule
                ,CASE WHEN AA.chargerule = 'ZZ' THEN '點數沖銷'
                     WHEN AA.chargerule = '0W' THEN '遊戲扣點'
                     WHEN AA.chargerule = '0N' THEN '自由點數'
                     WHEN AA.chargerule = '00' THEN '天堂月服12時'
                     WHEN AA.chargerule = '02' THEN '天堂月服2時'
                     WHEN AA.chargerule = '05' THEN '天堂月服31天'
                     ELSE AA.chargerule END  AS chargerule_name
                ,AA.chargepoints,AA.chargeservicepoints,AA.chargepoints + AA.chargeservicepoints AS totalpoints
                ,AA.memo,AA.logouttime,BB.istest  
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
                WHERE dt = '[:DateNoLine]'
                AND   SUBSTR(logouttime ,0,10) = '[:DateLine]'
                AND   AA.servicecode IN ('999995','999973')
                AND   (AA.chargepoints != 0 OR chargeservicepoints != 0)
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='16002')
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , CommonData_7 AS CommonData_7, CommonData_8 AS CommonData_8, CommonData_9 AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , totalpoints AS UniqueInt_1, chargepoints AS UniqueInt_2, chargeservicepoints AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , ipaddress AS UniqueStr_1, 'point' AS UniqueStr_2, chargerule AS UniqueStr_3
            , chargerule_name AS UniqueStr_4, memo AS UniqueStr_5, NULL AS UniqueStr_6
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
            , logouttime AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1
            WHERE istest = 0 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]


