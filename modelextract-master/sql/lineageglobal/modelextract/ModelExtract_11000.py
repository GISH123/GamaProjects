import time


class ModelExtract_11000 :

    # 伺服器編號DB跟Log相差100，join時要注意
    # layer_01
    @classmethod
    def insert11001DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11001 ModelExtract Data
            FROM (
                SELECT a.actor_account_name AS CommonData_1,a.actor_world AS CommonData_2,a.actor_id AS CommonData_3
                ,a.actor_name AS CommonData_4,nvl(b.class_tw_nm,a.actor_class) AS CommonData_5
                ,MAX(a.actor_level) AS CommonData_6,MAX(acc_in_cnt) AS UniqueInt_1,MAX(char_in_cnt) AS UniqueInt_2
                ,MAX(acc_out_cnt) AS UniqueInt_3,MAX(char_out_cnt) AS UniqueInt_4,c.createdate AS UniqueStr_2
                ,d.create_date AS UniqueStr_3
                FROM (
                    SELECT actor_account_name,actor_world,actor_id,actor_name,actor_class,actor_level,
                    COUNT(actor_account_name) OVER(PARTITION BY actor_account_name) AS acc_in_cnt,
                    COUNT(actor_id) OVER(PARTITION BY actor_world,actor_id) AS char_in_cnt,
                    NULL AS acc_out_cnt,NULL AS char_out_cnt
                    FROM lineage.[:LinageLogD]
                    WHERE logid IN (1003)
                    AND   `date` >= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND   `date` <= if(DATE_ADD('[:DateLine]',2) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',2),'yyyyMMdd'))
                    AND   SUBSTR(acttime,0,10) = '[:DateLine]'
                    UNION ALL
                    SELECT actor_account_name,actor_world,actor_id,actor_name,actor_class,actor_level,
                    NULL AS acc_in_cnt,NULL AS char_in_cnt,
                    COUNT(actor_account_name) OVER(PARTITION BY actor_account_name) AS acc_out_cnt,
                    COUNT(actor_id) OVER(PARTITION BY actor_world,actor_id) AS char_out_cnt
                    FROM lineage.[:LinageLogD]
                    WHERE logid IN (1004)
                    AND   `date` >= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND   `date` <= if(DATE_ADD('[:DateLine]',2) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',2),'yyyyMMdd'))
                    AND   SUBSTR(acttime,0,10) = '[:DateLine]'
                ) a
                LEFT OUTER JOIN lineage_extract.[:GameName]_class_data b	
                ON a.actor_class = b.class_id
                LEFT OUTER JOIN (    
                    SELECT DISTINCT account,createdate
                    FROM lineage_all.[:LinageUserInfo]
                    WHERE dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-1),'yyyyMMdd')
                    AND   dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                    ) c
                ON a.actor_account_name = c.account     
                LEFT OUTER JOIN (    
                    SELECT DISTINCT real_account,user_id,create_date,uid,world
                    FROM  lineage_all.[:LinageUserData]
                    WHERE dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-1),'yyyyMMdd')
                    AND   dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                    ) d
                ON a.actor_account_name = d.real_account AND a.actor_id = d.uid AND cast(a.actor_world as int) = (cast(d.world as int)+160)
                GROUP BY a.actor_account_name,a.actor_world,a.actor_id,a.actor_name,a.actor_class,b.class_tw_nm
                ,c.createdate,d.create_date
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='11001') 
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, CommonData_3 AS CommonData_3
            , CommonData_4 AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , UniqueInt_4 AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , NULL AS UniqueStr_1, UniqueStr_2 AS UniqueStr_2, UniqueStr_3 AS UniqueStr_3
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
            WHERE CommonData_2 = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert11002DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11002 ModelExtract Data
            FROM (
                SELECT z.actor_account_name AS CommonData_1,z.actor_world AS CommonData_2,z.actor_id AS CommonData_3
                ,z.actor_name AS CommonData_4,z.class_tw_nm AS CommonData_5,MAX(z.actor_level) AS CommonData_6
                ,z.acc_ip_cnt AS UniqueInt_1,z.char_ip_cnt AS UniqueInt_2,y.ip_acc_cnt AS UniqueInt_3
                ,y.ip_char_cnt AS UniqueInt_4,IP AS UniqueStr_1
                FROM(  
                    SELECT a.actor_account_name,a.actor_world,a.actor_id,a.actor_name
                    ,nvl(b.class_tw_nm,a.actor_class) AS class_tw_nm,a.actor_level,a.data1_str AS IP
                    ,COUNT(a.data1_str) OVER(PARTITION BY a.actor_account_name,a.data1_str) as acc_ip_cnt
                    ,COUNT(a.data1_str) OVER(PARTITION BY a.actor_id,a.data1_str) as char_ip_cnt 
                    FROM lineage.[:LinageLogD] a 
                    LEFT OUTER JOIN lineage_extract.[:GameName]_class_data b
                    ON a.actor_class = b.class_id 
                    WHERE logid in (1003)
                    AND   `date` >= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND   `date` <= if(DATE_ADD('[:DateLine]',2) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',2),'yyyyMMdd'))
                    AND   SUBSTR(acttime,0,10) = '[:DateLine]'
                ) z  
                LEFT OUTER JOIN (  
                    SELECT data1_str as IP,COUNT(DISTINCT actor_account_name) AS ip_acc_cnt
                    ,COUNT(DISTINCT actor_id) AS ip_char_cnt
                    FROM lineage.[:LinageLogD] 
                    WHERE logid in (1003)
                    AND   `date` >= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND   `date` <= if(DATE_ADD('[:DateLine]',2) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',2),'yyyyMMdd'))
                    AND   SUBSTR(acttime,0,10) = '[:DateLine]'
                    GROUP BY data1_str
                ) y  
                ON z.IP = y.IP
                WHERE z.IP IS NOT NULL
                GROUP BY z.actor_account_name,z.actor_world,z.actor_id,z.actor_name,z.class_tw_nm
                ,z.IP,z.acc_ip_cnt,z.char_ip_cnt,y.ip_acc_cnt,y.ip_char_cnt
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='11002') 
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, CommonData_3 AS CommonData_3
            , CommonData_4 AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , UniqueInt_4 AS UniqueInt_4
            ,cast(split(split(UniqueStr_1,':')[0],'\\\\.')[0] as bigint)*256*256*256
            + cast(split(split(UniqueStr_1,':')[0],'\\\\.')[1] as bigint)*256*256
            + cast(split(split(UniqueStr_1,':')[0],'\\\\.')[2] as bigint)*256
            + cast(split(split(UniqueStr_1,':')[0],'\\\\.')[3] as bigint) AS UniqueInt_5
            , NULL AS UniqueInt_6
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
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1
            WHERE CommonData_2 = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert11005DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11005 ModelExtract Data
            FROM (
                SELECT a.actor_account_name AS CommonData_1,a.actor_world AS CommonData_2,a.actor_id AS CommonData_3
                ,a.actor_name AS CommonData_4,nvl(b.class_tw_nm,a.actor_class) AS CommonData_5,a.actor_level AS CommonData_6
                ,datediff(a.acttime,a.lagtime) AS UniqueInt_1
                ,unix_timestamp(a.acttime,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.lagtime,'yyyy-MM-dd HH:mm:ss') AS UniqueInt_2
                ,IP AS UniqueStr_1,c.createdate AS UniqueStr_2,d.create_date AS UniqueStr_3
                ,a.lagtime AS UniqueTime_1,a.acttime AS UniqueTime_2
                FROM ( 
                    SELECT actor_account_name,actor_world,actor_id,actor_name,actor_class,actor_level,logid,acttime,
                    LAG(acttime) OVER(PARTITION BY actor_account_name,actor_world,actor_id order by acttime,logid) AS lagtime,
                    LAG(data1_str) OVER(PARTITION BY actor_account_name,actor_world,actor_id order by acttime,logid) AS IP
                    FROM lineage.[:LinageLogD]
                    WHERE logid in (1003,1004)
                    AND   `date` >= DATE_FORMAT(DATE_ADD('[:DateLine]',-14),'yyyyMMdd') --維護最長在14日內有兩次，因計算在線所以登入要往前14日
                    AND   `date` <= if(DATE_ADD('[:DateLine]',14) >= DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd')
                                        , DATE_FORMAT(DATE_ADD(CURRENT_DATE,-1),'yyyyMMdd')
                                        , DATE_FORMAT(DATE_ADD('[:DateLine]',14),'yyyyMMdd')) --維護最長在14日內有兩次，因計算在線所以登出要往後14日
                ) a	
                LEFT OUTER JOIN lineage_extract.[:GameName]_class_data b	
                ON a.actor_class = b.class_id	
                LEFT OUTER JOIN (
                    SELECT DISTINCT account,createdate
                    FROM lineage_all.[:LinageUserInfo]
                    WHERE dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-1),'yyyyMMdd')
                    AND   dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                    ) c	
                ON a.actor_account_name = c.account 	
                LEFT OUTER JOIN (
                    SELECT DISTINCT real_account,user_id,create_date,world
                    FROM  lineage_all.[:LinageUserData]
                    WHERE dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-1),'yyyyMMdd')
                    AND   dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                    ) d	
                ON a.actor_account_name = d.real_account AND a.actor_name = d.user_id AND cast(a.actor_world as int) = (cast(d.world as int)+160)
                WHERE logid = 1004	
                AND   acttime >= DATE_ADD('[:DateLine]',0)
                AND   lagtime < DATE_ADD('[:DateLine]',1)
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='11005') 
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, CommonData_3 AS CommonData_3
            , CommonData_4 AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , UniqueStr_1 AS UniqueStr_1, UniqueStr_2 AS UniqueStr_2, UniqueStr_3 AS UniqueStr_3
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
            WHERE CommonData_2 = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    # layer_02
    @classmethod
    def insert11103DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11103 ModelExtract Data
            FROM (    
                SELECT CommonData_1,CommonData_2,CommonData_3,CommonData_4,CommonData_5,CommonData_6
                ,UniqueStr_1,UniqueStr_2,UniqueStr_3
                ,CASE WHEN UniqueTime_1 < '[:DateLine]' THEN '[:DateLine] 00:00:00.000' ELSE CAST(UniqueTime_1 AS STRING) END AS login_time
                ,CASE WHEN UniqueTime_2 >= DATE_ADD('[:DateLine]',1) THEN '[:DateLine] 23:59:59.999' ELSE CAST(UniqueTime_2 AS STRING) END AS logout_time
                ,unix_timestamp((CASE WHEN UniqueTime_2 >= DATE_ADD('[:DateLine]',1) THEN '[:DateLine] 23:59:59.999' ELSE CAST(UniqueTime_2 AS STRING) END),'yyyy-MM-dd HH:mm:ss') - 
                unix_timestamp((CASE WHEN UniqueTime_1 < '[:DateLine]' THEN '[:DateLine] 00:00:00.000' ELSE CAST(UniqueTime_1 AS STRING) END),'yyyy-MM-dd HH:mm:ss') AS UniqueInt_2 
                FROM gtwpd.modelextract_modelextract 
                WHERE game = '[:GameName]'
                AND   dt = '[:DateNoLine]'
                AND   tablenumber = '11005'
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='11103')
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, CommonData_3 AS CommonData_3
            , CommonData_4 AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , NULL AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , UniqueStr_1 AS UniqueStr_1, UniqueStr_2 AS UniqueStr_2, UniqueStr_3 AS UniqueStr_3
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
            WHERE CommonData_2 = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert11802DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11802 ModelExtract Data
            FROM (    
                SELECT AAA.UniqueStr_1,AAA.UniqueInt_3,AAA.UniqueInt_4,AAA.UniqueInt_5
                ,BBB.UniqueStr_2,BBB.UniqueStr_3,BBB.UniqueStr_4,BBB.UniqueStr_5,BBB.UniqueStr_6
                ,BBB.UniqueStr_7,BBB.uniquedbl_1,BBB.uniquedbl_2 
                FROM (
                    SELECT DISTINCT UniqueStr_1,UniqueInt_3,UniqueInt_4,UniqueInt_5,tablenumber
                    FROM gtwpd.modelextract_modelextract
                    WHERE game = '[:GameName]'
                    AND   dt = '[:DateNoLine]'
                    AND   tablenumber = '11002'
                ) AAA
                INNER JOIN (
                    SELECT AA.commondata_7 AS UniqueStr_2,AA.uniquestr_4 AS UniqueStr_3,AA.uniquestr_5 AS UniqueStr_4
                    ,AA.UniqueStr_6 AS UniqueStr_5,AA.UniqueStr_7 AS UniqueStr_6,AA.UniqueStr_8 AS UniqueStr_7
                    ,AA.uniquedbl_1,AA.uniquedbl_2,AA.uniqueint_1,AA.uniqueint_2,AA.tablenumber
                    FROM gtwpd.modelextract_modelextract AA
                    INNER JOIN (
                        SELECT max(dt) AS dt
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '11854'
                    ) BB
                    ON AA.dt = BB.dt AND AA.tablenumber = '11854'              
                ) BBB ON (AAA.tablenumber + 852) = BBB.tablenumber
                WHERE AAA.UniqueInt_5 BETWEEN BBB.uniqueint_1 AND BBB.uniqueint_2
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11802')
            SELECT NULL AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , NULL AS UniqueInt_1, NULL AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , UniqueInt_4 AS UniqueInt_4, UniqueInt_5 AS UniqueInt_5, NULL AS UniqueInt_6
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
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert11806DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11806 ModelExtract Data
            FROM (    
                SELECT AAA.UniqueStr_1,AAA.UniqueInt_3,AAA.UniqueInt_4,AAA.UniqueInt_5
                ,BBB.UniqueStr_2,BBB.UniqueStr_3,BBB.UniqueStr_4,BBB.UniqueStr_5,BBB.UniqueStr_6
                ,BBB.UniqueStr_7,BBB.UniqueStr_8,BBB.UniqueStr_11,BBB.UniqueStr_12,BBB.UniqueStr_13
                ,BBB.UniqueDbl_1,BBB.UniqueDbl_2 
                FROM (
                    SELECT DISTINCT UniqueStr_1,UniqueInt_3,UniqueInt_4,UniqueInt_5,tablenumber
                    FROM gtwpd.modelextract_modelextract
                    WHERE game = '[:GameName]'
                    AND   dt = '[:DateNoLine]'
                    AND   tablenumber = '11002'
                ) AAA
                INNER JOIN (
                    SELECT AA.commondata_7 AS UniqueStr_2,AA.uniquestr_2 AS UniqueStr_3,AA.uniquestr_4
                    ,AA.uniquestr_5,AA.UniqueStr_6,AA.UniqueStr_7,AA.UniqueStr_8
                    ,AA.uniquestr_11,AA.UniqueStr_12,AA.UniqueStr_13
                    ,AA.uniquedbl_1,AA.uniquedbl_2,AA.uniqueint_1,AA.uniqueint_2,AA.tablenumber
                    FROM gtwpd.modelextract_modelextract AA
                    INNER JOIN (
                        SELECT max(dt) AS dt
                        FROM gtwpd.modelextract_modelextract 
                        WHERE tablenumber = '11861'
                    ) BB
                    ON AA.dt = BB.dt AND AA.tablenumber = '11861'              
                ) BBB ON (AAA.tablenumber + 859) = BBB.tablenumber
                WHERE split(AAA.uniquestr_1, '\\\\.')[0] = BBB.uniquestr_11
                AND   split(AAA.uniquestr_1, '\\\\.')[1] = BBB.uniquestr_12
                AND   split(AAA.uniquestr_1, '\\\\.')[2] = BBB.uniquestr_13
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11806')
            SELECT NULL AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , NULL AS UniqueInt_1, NULL AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , UniqueInt_4 AS UniqueInt_4, UniqueInt_5 AS UniqueInt_5, NULL AS UniqueInt_6
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
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]
    
    # layer_03
    @classmethod
    def insert11131DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11131 ModelExtract Data
            FROM ( 
                SELECT AAA.logtime , AAA.country , AAA.world, sum(AAA.ind) AS ind
                FROM (
                    SELECT date_format(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss') AS logtime
                    , AA.commondata_7 as country, AA.world, SUM(1) as ind
                    FROM gtwpd.modelextract_modelextract AA
                    WHERE AA.game = '[:GameName]'
                    AND   AA.dt = '[:DateNoLine]'
                    AND   AA.tablenumber = '11103'
                    GROUP BY date_format(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                    , AA.commondata_7, AA.world
                    UNION ALL
                    SELECT date_format(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss') AS logtime
                    , AA.commondata_7 as country, AA.world, SUM(-1) AS ind
                    FROM gtwpd.modelextract_modelextract AA
                    WHERE AA.game = '[:GameName]'
                    AND   AA.dt = '[:DateNoLine]'
                    AND   AA.tablenumber = '11103'
                    AND   date_format(AA.uniquetime_2,'HH:mm:ss') != '23:59:59'
                    GROUP BY date_format(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                    , AA.commondata_7, AA.world
                    UNION ALL
                    SELECT concat('[:DateLine] ',uniquestr_1) AS logtime, NULL AS country, AA.world, 0 AS ind
                    FROM gtwpd.modelextract_modelextract AA
                    WHERE AA.game='COMMON'
                    AND   AA.dt='00000000'
                    AND   AA.world='COMMON'
                    AND   AA.tablenumber='90001'
                ) AAA
                GROUP BY AAA.logtime, AAA.country, AAA.world
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='11131')
            SELECT NULL AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
            , country AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , SUM(ind) AS UniqueInt_1, NULL AS UniqueInt_2, NULL AS UniqueInt_3
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
            , logtime AS UniqueTime_1, NULL AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1 
            WHERE world in ('COMMON', '[:World]')
            GROUP BY logtime,country """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]