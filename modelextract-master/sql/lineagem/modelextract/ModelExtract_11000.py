import time


class ModelExtract_11000 :

    # layer_00
    @classmethod
    def insert11001DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11001 ModelExtract Data
            FROM (
                SELECT a.actor_account_id AS CommonData_1,a.actor_world AS CommonData_2,a.actor_id AS CommonData_3
                ,a.actor_name AS CommonData_4,b.class_tw_nm AS CommonData_5,MAX(a.actor_level) AS CommonData_6
                ,MAX(acc_in_cnt) AS UniqueInt_1,MAX(char_in_cnt) AS UniqueInt_2,MAX(acc_out_cnt) AS UniqueInt_3
                ,MAX(char_out_cnt) AS UniqueInt_4,c.game_acc_create_time AS UniqueStr_2,d.createdate AS UniqueStr_3
                FROM (
                    SELECT actor_account_id,actor_world,actor_id,actor_name,actor_class,actor_level,
                    COUNT(actor_account_id) OVER(PARTITION BY actor_account_id) AS acc_in_cnt,
                    COUNT(actor_id) OVER(PARTITION BY actor_world,actor_id) AS char_in_cnt,
                    NULL AS acc_out_cnt,NULL AS char_out_cnt
                    FROM mobile_lm.lm_gamelog_logid
                    WHERE plogid in (1003)
                    AND   plogdate = '[:DateNoLine]'
                    AND   log_detail_code NOT IN (3)
                    UNION ALL
                    SELECT actor_account_id,actor_world,actor_id,actor_name,actor_class,actor_level,
                    NULL AS acc_in_cnt,NULL AS char_in_cnt,
                    COUNT(actor_account_id) OVER(PARTITION BY actor_account_id) AS acc_out_cnt,
                    COUNT(actor_id) OVER(PARTITION BY actor_world,actor_id) AS char_out_cnt
                    FROM mobile_lm.lm_gamelog_logid
                    WHERE plogid in (1004)
                    AND   plogdate = '[:DateNoLine]'
                    AND   log_detail_code NOT IN (3)
                ) a
                LEFT OUTER JOIN mobile_lm.dlm_class b
                ON a.actor_class = b.class_cd
                LEFT OUTER JOIN mobile_lm.dlm_acc c
                ON a.actor_account_id = c.game_acc_id AND c.plogdate = '[:DateNoLine]'
                LEFT OUTER JOIN mobile_lm.lm_character d
                ON a.actor_id = d.characterguid AND a.actor_world = d.pserverno AND d.plogdate = '[:DateNoLine]'
                GROUP BY actor_account_id,actor_world,actor_id,actor_name,class_tw_nm
                ,c.game_acc_create_time,d.createdate
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
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
                SELECT z.actor_account_id AS CommonData_1,z.actor_world AS CommonData_2,z.actor_id AS CommonData_3
                ,z.actor_name AS CommonData_4,z.class_tw_nm AS CommonData_5,MAX(z.actor_level) AS CommonData_6
                ,z.acc_ip_cnt AS UniqueInt_1,z.char_ip_cnt AS UniqueInt_2,y.ip_acc_cnt AS UniqueInt_3
                ,y.ip_char_cnt AS UniqueInt_4,IP AS UniqueStr_1
                FROM(  
                    SELECT a.actor_account_id,a.actor_world,a.actor_id,a.actor_name,b.class_tw_nm,
                    a.actor_level,a.actor_str1 AS IP, 
                    COUNT(a.actor_str1) OVER(PARTITION BY a.actor_account_id,a.actor_str1) as acc_ip_cnt , 
                    COUNT(a.actor_str1) OVER(PARTITION BY a.actor_id,a.actor_str1) as char_ip_cnt 
                    FROM mobile_lm.lm_gamelog_logid a 
                    LEFT OUTER JOIN mobile_lm.dlm_class b 
                    ON a.actor_class = b.class_cd 
                    WHERE plogid in (1003) 
                    AND   plogdate = '[:DateNoLine]' 
                    AND   log_detail_code NOT IN (3)
                ) z  
                LEFT OUTER JOIN (  
                    SELECT actor_str1 as IP,COUNT(DISTINCT actor_account_id) AS ip_acc_cnt,
                    COUNT(DISTINCT actor_id) AS ip_char_cnt
                    FROM mobile_lm.lm_gamelog_logid 
                    WHERE plogid in (1003) 
                    AND   plogdate = '[:DateNoLine]' 
                    AND   log_detail_code NOT IN (3)
                    GROUP BY actor_str1 
                ) y  
                ON z.IP = y.IP
                WHERE z.IP IS NOT NULL
                GROUP BY z.actor_account_id,z.actor_world,z.actor_id,z.actor_name,z.class_tw_nm,
                z.IP,z.acc_ip_cnt,z.char_ip_cnt,y.ip_acc_cnt,y.ip_char_cnt
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='11002') 
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
                SELECT a.actor_account_id AS CommonData_1,a.actor_world AS CommonData_2,a.actor_id AS CommonData_3
                ,a.actor_name AS CommonData_4,b.class_tw_nm AS CommonData_5,a.actor_level AS CommonData_6
                ,datediff(time ,lagtime) AS UniqueInt_1
                ,unix_timestamp(time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(lagtime,'yyyy-MM-dd HH:mm:ss') AS UniqueInt_2
                ,IP AS UniqueStr_1,c.game_acc_create_time AS UniqueStr_2,d.createdate AS UniqueStr_3
                ,lagtime AS UniqueTime_1,time AS UniqueTime_2
                FROM ( 
                    SELECT actor_account_id,actor_world,actor_id,actor_name,actor_class,actor_level,plogid,time,
                    LAG(time) OVER(PARTITION BY actor_account_id,actor_world,actor_id order by time,plogid) AS lagtime,
                    LAG(actor_str1) OVER(PARTITION BY actor_account_id,actor_world,actor_id order by time,plogid) AS IP
                    FROM mobile_lm.lm_gamelog_logid
                    WHERE plogid in (1003,1004)
                    AND   plogdate >= DATE_FORMAT(DATE_ADD('[:DateLine]',-14),'yyyyMMdd')
                    AND   plogdate <= DATE_FORMAT(DATE_ADD('[:DateLine]',14),'yyyyMMdd')
                    AND   log_detail_code NOT IN (3)
                ) a 
                LEFT OUTER JOIN mobile_lm.dlm_class b 
                ON a.actor_class = b.class_cd 
                LEFT OUTER JOIN mobile_lm.dlm_acc c 
                ON a.actor_account_id = c.game_acc_id AND c.plogdate = '[:DateNoLine]' 
                LEFT OUTER JOIN mobile_lm.lm_character d 
                ON a.actor_id = d.characterguid AND a.actor_world = d.pserverno AND d.plogdate = '[:DateNoLine]' 
                WHERE plogid = 1004 
                AND   time >= DATE_ADD('[:DateLine]',0) 
                AND   lagtime < DATE_ADD('[:DateLine]',1)
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
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

    # layer_01
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
                FROM rcenter.gama_bda_model_extract 
                WHERE game = '[:GameName]'
                AND   dt = '[:DateNoLine]'
                AND   tablenumber = '11005'
            ) tmp """
        mutiInsertSQLCode = """
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
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
    
    # layer_02
    def insert11131DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 11131 ModelExtract Data
            FROM ( 
                SELECT AAA.logtime , AAA.country , AAA.world, sum(AAA.ind) AS ind
                FROM (
                    SELECT date_format(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss') AS logtime
                    , AA.commondata_7 as country, AA.world, SUM(1) as ind
                    FROM rcenter.gama_bda_model_extract AA
                    WHERE AA.game = '[:GameName]'
                    AND   AA.dt = '[:DateNoLine]'
                    AND   AA.tablenumber = '11103'
                    GROUP BY date_format(AA.uniquetime_1,'yyyy-MM-dd HH:mm:ss')
                    , AA.commondata_7, AA.world
                    UNION ALL
                    SELECT date_format(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss') AS logtime
                    , AA.commondata_7 as country, AA.world, SUM(-1) AS ind
                    FROM rcenter.gama_bda_model_extract AA
                    WHERE AA.game = '[:GameName]'
                    AND   AA.dt = '[:DateNoLine]'
                    AND   AA.tablenumber = '11103'
                    AND   date_format(AA.uniquetime_2,'HH:mm:ss') != '23:59:59'
                    GROUP BY date_format(AA.uniquetime_2,'yyyy-MM-dd HH:mm:ss')
                    , AA.commondata_7, AA.world
                    UNION ALL
                    SELECT concat('[:DateLine] ',uniquestr_1) AS logtime, NULL AS country, AA.world, 0 AS ind
                    FROM rcenter.gama_bda_model_extract AA
                    WHERE AA.tablenumber = '90001'
                ) AAA
                GROUP BY AAA.logtime, AAA.country, AAA.world
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
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