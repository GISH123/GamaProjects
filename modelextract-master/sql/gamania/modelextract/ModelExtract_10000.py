import time
import datetime
from sql.common.modelextract.ModelExtract_10000 import ModelExtract_10000 as ModelExtract_10000_Common


class ModelExtract_10000(ModelExtract_10000_Common):

    def insert10001DataSQL(self, makeInfo):
        makeTime = makeInfo["makeTime"]
        print(makeTime)
        if makeTime >= datetime.datetime(2021, 1, 1, 0, 0, 0, 0):
            return self.__insert10001DataSQL_new_20210101(makeInfo)
        else:
            return self.__insert10001DataSQL_old_20191201_to_20201231(makeInfo)

    def __insert10001DataSQL_new_20210101(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 10001 ModelExtract Data
            FROM (
               	SELECT DISTINCT 
                    LOWER(AAAA.mainaccountid) AS mainaccountid
                    , CCCC.createtime AS main_createtime
                    , LOWER(AAAA.serviceaccountid) AS serviceaccountid
                    , AAAA.createtime AS service_createtime
                    , LOWER(BBBB.bfopenid) AS bfopenid
                    , LOWER(BBBB.globalsn) AS globalsn
                    , BBBB.createtime AS newbeanfun_createtime
                    , AAAA.servicecode
                    , EEEE.displayname
                    , CASE WHEN DDDD.mainaccountid IS NULL THEN 0 ELSE 1 END AS istest
                FROM (
                    SELECT 
                        LOWER( AA.mainaccount) AS mainaccountid
                        , LOWER(AA.serviceaccountid) AS serviceaccountid 
                        , MAX(AA.createtime) AS createtime 
                        , AA.servicecode
                    FROM bf_extract.beanfundb_d_serviceaccount_f AA
                    WHERE 1 = 1 
                        AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd')     
                        AND AA.dt <= if(
                            DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                            , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                            , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                        )
                    GROUP BY 
                        LOWER(AA.mainaccount)
                        , LOWER(AA.serviceaccountid)
                        , AA.servicecode
                ) AAAA 
                LEFT OUTER JOIN (    
                    SELECT 
                        AAA.mainaccountid 
                        , MAX(AAA.bfopenid) AS bfopenid 
                        , MAX(AAA.globalsn) as globalsn
                        , MAX(AAA.createtime) AS createtime
                        , MAX(CASE WHEN AAA.bfopenid IS NOT NULL THEN 'bfopenid' ELSE 'bfstarid' END) AS bfidtype
                    FROM ( 
                        SELECT 
                            LOWER( AA.mainaccountid) AS mainaccountid 
                            , FIRST_VALUE(BB.bfopenid) OVER(PARTITION BY LOWER(AA.mainaccountid) ORDER BY AA.dt DESC) AS bfopenid
                            , FIRST_VALUE(AA.globalsn) OVER(PARTITION BY LOWER(AA.mainaccountid) ORDER BY AA.dt DESC) AS globalsn
                            , AA.createtime
                            , AA.status
                        FROM bf_extract.beanfundb_bfapp_mainaccount AA
                        LEFT OUTER JOIN bf_extract.gtwbf5035_bfapp_bfapp_userdataextend BB ON 1 = 1
                            AND AA.bfappuid = BB.bfappuid
                            AND AA.dt = BB.dt
                        where 1 = 1 
                            AND AA.dt <= if(
                                DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                            )
                            AND AA.status = 1 --有綁定的bf openid
                    ) AAA
                    GROUP BY 
                        AAA.mainaccountid 	
                ) BBBB ON 1 = 1 
                    AND LOWER(AAAA.mainaccountid) = LOWER(BBBB.mainaccountid)    
                LEFT OUTER JOIN (    
                    SELECT LOWER(mainaccountid) AS mainaccountid,MAX(createtime) AS createtime
                    FROM bf_extract.beanfundb_d_mainaccount_f_fix
                    WHERE 1 = 1 
                        AND dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd')      
                        AND dt <= if(
                            DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                            , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                            , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                        )
                    GROUP BY 
                        LOWER(mainaccountid)
                ) CCCC ON 1 = 1 
                    AND LOWER(AAAA.mainaccountid) = LOWER(CCCC.mainaccountid) 
                LEFT OUTER JOIN (
                    SELECT DISTINCT 
                        LOWER(mainaccountid) AS mainaccountid
                    FROM bf_extract.beanfundb_beanfun_testmainaccount
                    WHERE 1 = 1 
                        AND dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd')       
                        AND dt <= if(
                            DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                            , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                            , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                        )
                ) DDDD ON 1 = 1 
                    AND LOWER(AAAA.mainaccountid) = LOWER(DDDD.mainaccountid)
                LEFT OUTER JOIN (    
                    SELECT DISTINCT 
                        servicecode
                        , displayname
                    FROM bf_extract.beanfundb_service_info
                    WHERE 1 = 1 
                        AND dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd')                    
                        AND dt <= if(
                            DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                            , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                            , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                        )
                ) EEEE ON 1 = 1
                    AND AAAA.servicecode = EEEE.servicecode 
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='10001')
            SELECT 
                serviceaccountid AS CommonData_1
                , globalsn AS CommonData_2
                , NULL AS CommonData_3
                , NULL AS CommonData_4
                , mainaccountid AS CommonData_5
                , bfopenid AS CommonData_6
                , 'TW' AS CommonData_7
                , servicecode AS CommonData_8
                , displayname AS CommonData_9
                , 'gamania' AS CommonData_10
                , istest AS CommonData_11
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
                , service_createtime AS UniqueStr_2
                , main_createtime AS UniqueStr_3
                , NULL AS UniqueStr_4
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
            """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    def __insert10001DataSQL_old_20191201_to_20201231(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 10001 ModelExtract Data
            FROM (
                SELECT DISTINCT LOWER(AA.mainaccountid) AS mainaccountid,CC.createtime AS main_createtime
                ,LOWER(AA.serviceaccountid) AS serviceaccountid,AA.createtime AS service_createtime
                ,LOWER(BB.newbeanfunglobalsn) AS newbeanfunglobalsn,BB.createtime AS newbeanfun_createtime
                ,AA.servicecode,EE.displayname,CASE WHEN DD.mainaccountid IS NULL THEN 0 ELSE 1 END AS istest
                FROM (
                    SELECT LOWER(mainaccount) AS mainaccountid,LOWER(serviceaccountid) AS serviceaccountid 
                    ,MAX(createtime) AS createtime ,servicecode
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
                    GROUP BY LOWER(mainaccount),LOWER(serviceaccountid),servicecode
                ) AA 
                LEFT OUTER JOIN (    
                    SELECT mainaccountid,newbeanfunglobalsn,MAX(createtime) AS createtime
                    FROM(
                        SELECT LOWER(mainaccountid) AS mainaccountid 
                        ,FIRST_VALUE(newbeanfunglobalsn) OVER(PARTITION BY LOWER(mainaccountid) ORDER BY updatetime DESC) AS newbeanfunglobalsn
                        ,createtime,status
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
                    GROUP BY mainaccountid,newbeanfunglobalsn
                ) BB    
                ON LOWER(AA.mainaccountid) = LOWER(BB.mainaccountid)    
                LEFT OUTER JOIN (    
                    SELECT LOWER(mainaccountid) AS mainaccountid,MAX(createtime) AS createtime
                    FROM bf_extract.beanfundb_d_mainaccount_f_fix
                    WHERE dt >= if(DATE_ADD('[:DateLine]',-3) <= '2020-04-28'  --資料從2020-04-28開始
                                    , '20200428'
                                    , DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd'))                    
                    AND   dt <= if(DATE_ADD('[:DateLine]',-3) <= '2020-04-28'  --資料從2020-04-28開始
                                , '20200428'
                                , if(DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                    , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                    , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                                  )
                                )
                    GROUP BY LOWER(mainaccountid)
                ) CC    
                ON LOWER(AA.mainaccountid) = LOWER(CC.mainaccountid) 
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
                ) DD
                ON LOWER(AA.mainaccountid) = LOWER(DD.mainaccountid)
                LEFT OUTER JOIN (    
                    SELECT DISTINCT servicecode,displayname
                    FROM bf_extract.beanfundb_service_info
                    WHERE dt >= if(DATE_ADD('[:DateLine]',-3) <= '2020-05-11'  --資料從2020-05-11開始
                                    , '20200511'
                                    , DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd'))                    
                    AND   dt <= if(DATE_ADD('[:DateLine]',-3) <= '2020-05-11'  --資料從2020-05-11開始
                                , '20200511'
                                , if(DATE_ADD('[:DateLine]',3) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                    , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                    , DATE_FORMAT(DATE_ADD('[:DateLine]',3),'yyyyMMdd')
                                  )
                                )
                ) EE
                ON AA.servicecode = EE.servicecode 
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='10001')
            SELECT serviceaccountid AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, mainaccountid AS CommonData_5, newbeanfunglobalsn AS CommonData_6
            , 'TW' AS CommonData_7, servicecode AS CommonData_8, displayname AS CommonData_9
            , 'gamania' AS CommonData_10, istest AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , NULL AS UniqueInt_1, NULL AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , NULL AS UniqueStr_1, service_createtime AS UniqueStr_2, main_createtime AS UniqueStr_3
            , newbeanfun_createtime AS UniqueStr_4, NULL AS UniqueStr_5, NULL AS UniqueStr_6
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
            , array(NULL) AS UniqueArray_1, array(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]