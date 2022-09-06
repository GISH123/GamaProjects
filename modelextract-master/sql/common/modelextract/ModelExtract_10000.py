import time
import datetime

class ModelExtract_10000 :

    @classmethod
    def insert10001DataSQL(self, makeInfo):
        makeTime = makeInfo["makeTime"]
        print(makeTime)
        if makeTime >= datetime.datetime(2021, 1, 1, 0, 0, 0, 0):
            return self.__insert10001DataSQL_new_20210101(makeInfo)
        else:
            return self.__insert10001DataSQL_old_20191201_to_20201231(makeInfo)

    @classmethod
    def __insert10001DataSQL_new_20210101(self, makeInfo):
        orderInsertSQLCode1 = """ 
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='10001')
            SELECT 
                AA.CommonData_1 AS CommonData_1
                , NULL AS CommonData_2
                , NULL AS CommonData_3
                , NULL AS CommonData_4
                , AA.CommonData_5 AS CommonData_5
                , AA.CommonData_6 AS CommonData_6
                , AA.CommonData_7 AS CommonData_7
                , AA.CommonData_8 AS CommonData_8
                , AA.CommonData_9 AS CommonData_9
                , AA.CommonData_10 AS CommonData_10
                , AA.CommonData_11 AS CommonData_11
                , NULL AS CommonData_12
                , NULL AS CommonData_13
                , NULL AS CommonData_14
                , NULL AS CommonData_15
                , NULL AS UniqueInt_1
                , NULL AS UniqueInt_2
                , NULL AS UniqueInt_3
                , NULL AS UniqueInt_4
                , NULL AS UniqueInt_5
                , NULL AS UniqueInt_6
                , NULL AS UniqueInt_7
                , NULL AS UniqueInt_8
                , NULL AS UniqueInt_9
                , NULL AS UniqueInt_10
                , NULL AS UniqueInt_11
                , NULL AS UniqueInt_12
                , NULL AS UniqueInt_13
                , NULL AS UniqueInt_14
                , NULL AS UniqueInt_15
                , NULL AS UniqueStr_1
                , AA.UniqueStr_2 AS UniqueStr_2
                , AA.UniqueStr_3 AS UniqueStr_3
                , AA.UniqueStr_4 AS UniqueStr_4
                , NULL AS UniqueStr_5
                , NULL AS UniqueStr_6
                , NULL AS UniqueStr_7
                , NULL AS UniqueStr_8
                , NULL AS UniqueStr_9
                , NULL AS UniqueStr_10
                , NULL AS UniqueStr_11
                , NULL AS UniqueStr_12
                , NULL AS UniqueStr_13
                , NULL AS UniqueStr_14
                , NULL AS UniqueStr_15
                , NULL AS UniqueStr_16
                , NULL AS UniqueStr_17
                , NULL AS UniqueStr_18
                , NULL AS UniqueStr_19
                , NULL AS UniqueStr_20
                , NULL AS UniqueDBL_1
                , NULL AS UniqueDBL_2
                , NULL AS UniqueDBL_3
                , NULL AS UniqueDBL_4
                , NULL AS UniqueDBL_5
                , NULL AS UniqueDBL_6
                , NULL AS UniqueDBL_7
                , NULL AS UniqueDBL_8
                , NULL AS UniqueDBL_9
                , NULL AS UniqueDBL_10
                , NULL AS UniqueDBL_11
                , NULL AS UniqueDBL_12
                , NULL AS UniqueDBL_13
                , NULL AS UniqueDBL_14
                , NULL AS UniqueDBL_15
                , NULL AS UniqueDBL_16
                , NULL AS UniqueDBL_17
                , NULL AS UniqueDBL_18
                , NULL AS UniqueDBL_19
                , NULL AS UniqueDBL_20
                , NULL AS UniqueTime_1
                , NULL AS UniqueTime_2
                , NULL AS UniqueTime_3
                , NULL AS OtherStr_1
                , NULL AS OtherStr_2
                , NULL AS OtherStr_3
                , NULL AS OtherStr_4
                , NULL AS OtherStr_5
                , NULL AS OtherStr_6
                , NULL AS OtherStr_7
                , NULL AS OtherStr_8
                , NULL AS OtherStr_9
                , NULL AS OtherStr_10
                , array(NULL) AS UniqueArray_1
                , array(NULL) AS UniqueArray_2
                , NULL AS UniqueJson_1 
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1 
                AND AA.game= 'gamania'
                AND AA.dt= '[:DateNoLine]'
                AND AA.world= 'COMMON'
                AND AA.tablenumber='10001'
                AND AA.CommonData_8 = '[:ServiceCode]';
            """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def __insert10001DataSQL_old_20191201_to_20201231(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelExtract 10001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='10001')
            SELECT
                AAA.serviceaccountid AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
                , NULL AS CommonData_4, AAA.mainaccountid AS CommonData_5, AAA.newbeanfunglobalsn AS CommonData_6
                , 'TW' AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
                , 'gamania' AS CommonData_10, AAA.istest AS CommonData_11, NULL AS CommonData_12
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
                , NULL AS UniqueDBL_1, NULL AS UniqueDBL_2, NULL AS UniqueDBL_3
                , NULL AS UniqueDBL_4, NULL AS UniqueDBL_5, NULL AS UniqueDBL_6
                , NULL AS UniqueDBL_7, NULL AS UniqueDBL_8, NULL AS UniqueDBL_9
                , NULL AS UniqueDBL_10, NULL AS UniqueDBL_11, NULL AS UniqueDBL_12
                , NULL AS UniqueDBL_13, NULL AS UniqueDBL_14, NULL AS UniqueDBL_15
                , NULL AS UniqueDBL_16, NULL AS UniqueDBL_17, NULL AS UniqueDBL_18
                , NULL AS UniqueDBL_19, NULL AS UniqueDBL_20
                , NULL AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
                , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
                , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
                , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
                , NULL AS OtherStr_10
                , array(NULL) AS UniqueArray_1, array(NULL) AS UniqueArray_2, NULL AS UniqueJson_1
            FROM (
                SELECT DISTINCT LOWER(AA.mainaccountid) AS mainaccountid
                ,LOWER(AA.serviceaccountid) AS serviceaccountid,LOWER(BB.newbeanfunglobalsn) AS newbeanfunglobalsn
                ,CASE WHEN CC.mainaccountid IS NULL THEN 0 ELSE 1 END AS istest
                FROM (
                    SELECT DISTINCT LOWER(mainaccount) AS mainaccountid,LOWER(serviceaccountid) AS serviceaccountid
                    FROM bf_extract.beanfundb_d_serviceaccount_f
                    WHERE dt >= if(DATE_ADD('[:DateLine]',-3) <= '2020-05-06'  --資料從2020-05-06開始
                                    , '20200506'
                                    , DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd'))                    
                    AND   dt <= if(DATE_ADD('[:DateLine]',-3) <= '2020-05-06'  --資料從2020-05-06開始
                                , '20200506'
                                , if(DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                    , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                    , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                                  )
                                )
                    AND   servicecode = '[:ServiceCode]'
                ) AA 
                LEFT OUTER JOIN (	
                    SELECT DISTINCT mainaccountid,newbeanfunglobalsn
                    FROM(
                        SELECT LOWER(mainaccountid) AS mainaccountid ,status
                        ,FIRST_VALUE(newbeanfunglobalsn) OVER(PARTITION BY LOWER(mainaccountid) ORDER BY updatetime DESC) AS newbeanfunglobalsn
                        FROM bf_extract.beanfundb_bfapp_mainaccount
                        WHERE dt >= if(DATE_ADD('[:DateLine]',-3) <= '2020-05-06'  --資料從2020-05-06開始
                                        , '20200506'
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd'))                    
                        AND   dt <= if(DATE_ADD('[:DateLine]',-3) <= '2020-05-06'  --資料從2020-05-06開始
                                    , '20200506'
                                    , if(DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                                      )
                                    )
                    ) tmp
                    WHERE status = 1 --有綁定的bf openid
                ) BB	
                ON LOWER(AA.mainaccountid) = LOWER(BB.mainaccountid)	
                LEFT OUTER JOIN (
                    SELECT DISTINCT LOWER(mainaccountid) AS mainaccountid
                    FROM bf_extract.beanfundb_beanfun_testmainaccount
                    WHERE dt >= if(DATE_ADD('[:DateLine]',-3) <= '2020-08-20'  --資料從2020-08-20開始
                                    , '20200820'
                                    , DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd'))                    
                    AND   dt <= if(DATE_ADD('[:DateLine]',-3) <= '2020-08-20'  --資料從2020-08-20開始
                                , '20200820'
                                , if(DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                    , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                    , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                                  )
                                )
                ) CC
                ON LOWER(AA.mainaccountid) = LOWER(CC.mainaccountid)
            ) AAA ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

