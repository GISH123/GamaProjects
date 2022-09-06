import time


class ModelExtract_16000 :

    # layer_00
    @classmethod
    def insert16001DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 16001 ModelExtract Data
            FROM (
                SELECT actor_account_id AS CommonData_1
                ,SUM(cash_twd) AS UniqueDbl_1,SUM(buy_qty) AS UniqueInt_2
                ,pg_goods_nm AS UniqueStr_1,currency_nm AS UniqueStr_2
                ,buymon AS UniqueStr_3
                FROM ( 
                    SELECT actor_account_id,pg_goods_nm,'TWD' AS currency_nm,cash_twd,buy_qty,buymon
                    FROM rcenter.gama_scheduler_AccInGameCash
                    WHERE plogdate = '[:DateNoLine]'
                    UNION ALL
                    SELECT actor_account_id,pg_goods_nm,'GASH' AS currency_nm,cash_twd,buy_qty,buymon
                    FROM rcenter.gama_scheduler_AccCouponCash
                    WHERE plogdate = '[:DateNoLine]'
                ) a
                GROUP BY actor_account_id,pg_goods_nm,currency_nm,buymon
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='16001') 
            SELECT CommonData_1 AS CommonData_1, NULL AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
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
            , UniqueDbl_1 AS UniqueDbl_1, NULL AS UniqueDbl_2, NULL AS UniqueDbl_3
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
            """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]

    @classmethod
    def insert16002DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 16002 ModelExtract Data
            FROM (
                SELECT a.actor_account_id AS CommonData_1,a.actor_world AS CommonData_2,a.actor_id AS CommonData_3
                ,d.name AS CommonData_4,b.class_tw_nm AS CommonData_5,a.actor_level AS CommonData_6
                ,total_Payamt AS UniqueInt_1,buy_qty AS UniqueInt_2,a.ps_goods_cd AS UniqueInt_3
                ,c.ps_goods_nm AS UniqueStr_1,BuyMon AS UniqueStr_2
                FROM rcenter.gama_scheduler_AccCharBuy_DiamItem a
                LEFT OUTER JOIN mobile_lm.dlm_class b
                ON a.actor_class = b.class_cd
                LEFT OUTER JOIN mobile_lm.dlm_goods c
                ON a.ps_goods_cd = c.ps_goods_cd
                LEFT OUTER JOIN mobile_lm.lm_character d
                ON a.actor_id = d.characterguid AND a.actor_world = d.pserverno AND d.plogdate = '[:DateNoLine]'
                WHERE a.plogdate = '[:DateNoLine]'
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='16002') 
            SELECT CommonData_1 AS CommonData_1, CommonData_2 AS CommonData_2, CommonData_3 AS CommonData_3
            , CommonData_4 AS CommonData_4, CommonData_5 AS CommonData_5, CommonData_6 AS CommonData_6
            , NULL AS CommonData_7, NULL AS CommonData_8, NULL AS CommonData_9
            , NULL AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , UniqueInt_1 AS UniqueInt_1, UniqueInt_2 AS UniqueInt_2, UniqueInt_3 AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, NULL AS UniqueInt_11, NULL AS UniqueInt_12
            , NULL AS UniqueInt_13, NULL AS UniqueInt_14, NULL AS UniqueInt_15
            , UniqueStr_1 AS UniqueStr_1, UniqueStr_2 AS UniqueStr_2, NULL AS UniqueStr_3
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
    def insert16503DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 16503 ModelExtract Data
            FROM (
                SELECT DISTINCT *
                FROM (
                    SELECT CommonData_2, CommonData_8, CommonData_9, CommonData_10
                    ,SUM(UniqueInt_1) OVER(PARTITION BY CommonData_2,UniqueStr_10) AS total_amt
                    ,SUM(UniqueInt_2) OVER(PARTITION BY CommonData_2,UniqueStr_10) AS total_num
                    ,ROUND(STDDEV_SAMP(CAST(UniqueDbl_1 AS DOUBLE)) OVER(PARTITION BY CommonData_2,UniqueStr_10),6) AS STDDEV_price
                    ,SUM(1) OVER(PARTITION BY CommonData_2,UniqueStr_10) AS trade_cnt
                    ,ROUND(MIN(CAST(UniqueDbl_1 AS DOUBLE)) OVER(PARTITION BY CommonData_2,UniqueStr_10),6) AS min_price
                    ,ROUND(MAX(CAST(UniqueDbl_1 AS DOUBLE)) OVER(PARTITION BY CommonData_2,UniqueStr_10),6) AS max_price
                    ,ROUND(AVG(CAST(UniqueDbl_1 AS DOUBLE)) OVER(PARTITION BY CommonData_2,UniqueStr_10),6) AS avg_price
                    ,ROUND(PERCENTILE_APPROX(CAST(UniqueDbl_1 AS DOUBLE), 0.25) OVER(PARTITION BY CommonData_2,UniqueStr_10),6) AS Quartile_1
                    ,ROUND(PERCENTILE_APPROX(CAST(UniqueDbl_1 AS DOUBLE), 0.5) OVER(PARTITION BY CommonData_2,UniqueStr_10),6) AS Quartile_2
                    ,ROUND(PERCENTILE_APPROX(CAST(UniqueDbl_1 AS DOUBLE), 0.75) OVER(PARTITION BY CommonData_2,UniqueStr_10),6) AS Quartile_3
                    ,UniqueInt_11,UniqueInt_12,UniqueInt_13,UniqueInt_14,UniqueStr_6,UniqueStr_10
                    ,world
                    FROM rcenter.gama_bda_model_extract
                    WHERE game = '[:GameName]'
                    AND   dt = '[:DateNoLine]'
                    AND   tablenumber = '16509' 
                ) AA
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='16503') 
            SELECT NULL AS CommonData_1, CommonData_2 AS CommonData_2, NULL AS CommonData_3
            , NULL AS CommonData_4, NULL AS CommonData_5, NULL AS CommonData_6
            , NULL AS CommonData_7, CommonData_8 AS CommonData_8, CommonData_9 AS CommonData_9
            , CommonData_10 AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , total_amt AS UniqueInt_1, total_num AS UniqueInt_2, NULL AS UniqueInt_3
            , trade_cnt AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, UniqueInt_11 AS UniqueInt_11, UniqueInt_12 AS UniqueInt_12
            , UniqueInt_13 AS UniqueInt_13, UniqueInt_14 AS UniqueInt_14, NULL AS UniqueInt_15
            , NULL AS UniqueStr_1, NULL AS UniqueStr_2, NULL AS UniqueStr_3
            , NULL AS UniqueStr_4, NULL AS UniqueStr_5, NULL AS UniqueStr_6
            , NULL AS UniqueStr_7, NULL AS UniqueStr_8, NULL AS UniqueStr_9
            , UniqueStr_10 AS UniqueStr_10, NULL AS UniqueStr_11, NULL AS UniqueStr_12
            , NULL AS UniqueStr_13, NULL AS UniqueStr_14, NULL AS UniqueStr_15
            , NULL AS UniqueStr_16, NULL AS UniqueStr_17, NULL AS UniqueStr_18
            , NULL AS UniqueStr_19, NULL AS UniqueStr_20
            , STDDEV_price AS UniqueDbl_1, min_price AS UniqueDbl_2, max_price AS UniqueDbl_3
            , avg_price AS UniqueDbl_4, Quartile_1 AS UniqueDbl_5, Quartile_2 AS UniqueDbl_6
            , Quartile_3 AS UniqueDbl_7, NULL AS UniqueDbl_8, NULL AS UniqueDbl_9
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
    def insert16509DataSQL(self, makeInfo):
        fromSQLCode = """
            -- Make [:GameName] 16509 ModelExtract Data
            FROM (
                SELECT AA.actor_account_id,AA.actor_world,AA.actor_id,AA.actor_name,BB.class_tw_nm AS actor_class,AA.actor_level
                ,AA.entity_id AS item_id,CC.item_nm,CC.item_group_nm
                ,CONCAT(
                        CASE WHEN AA.entity_grade = 0 THEN '活動' WHEN AA.entity_grade = 1 THEN '一般' WHEN AA.entity_grade = 2 THEN '高級'    
                             WHEN AA.entity_grade = 3 THEN '稀有' WHEN AA.entity_grade = 4 THEN '英雄' WHEN AA.entity_grade = 5 THEN '傳說' 
                             WHEN AA.entity_grade = 32 THEN '神話' WHEN AA.entity_grade = 42 THEN 'BM專屬' ELSE AA.entity_grade END
                        ,' +',AA.entity_lv,' '
                        ,CASE WHEN AA.entity_bless = 1 THEN '正常' WHEN AA.entity_bless = 2 THEN '祝福' WHEN AA.entity_bless = 2 THEN '詛呪'
                              else AA.entity_bless END
                        ,' ',CC.item_nm,' '
                        ,CASE WHEN AA.entity_attribute = 0 THEN '無屬性' WHEN AA.entity_attribute < 300 THEN '火屬性'
                              WHEN AA.entity_attribute < 600 THEN '水屬性' WHEN AA.entity_attribute < 800 THEN '風屬性'
                              WHEN AA.entity_attribute < 1100 THEN '地屬性' ELSE AA.entity_attribute END
                        ,if(AA.entity_attribute = 0,'','('),if(AA.entity_attribute = 0,'',AA.entity_attribute),if(AA.entity_attribute = 0,'',')')
                        ) AS item
                ,AA.use2_num AS diam,AA.use1_num AS item_num,ROUND(CAST(AA.use2_num/AA.Use1_Num AS DOUBLE),6) AS item_price 
                ,AA.entity_bless,AA.entity_lv,AA.entity_grade,AA.entity_attribute
                ,CASE WHEN entity_grade = 0 THEN '活動'
                      WHEN entity_grade = 1 THEN '一般'    
                      WHEN entity_grade = 2 THEN '高級'    
                      WHEN entity_grade = 3 THEN '稀有' 
                      WHEN entity_grade = 4 THEN '英雄' 
                      WHEN entity_grade = 5 THEN '傳說' 
                      WHEN entity_grade = 32 THEN '神話'
                      WHEN entity_grade = 42 THEN 'BM專屬'
                      ELSE entity_grade END AS grade
                ,AA.target_account_id,DD.target_world,DD.target_id,DD.target_name
                ,AA.data1_str as sellid,AA.time AS buytime,DD.selltime
                FROM mobile_lm.lm_gamelog_logid AA
                LEFT OUTER JOIN mobile_lm.dlm_class BB
                ON AA.actor_class = BB.class_cd
                LEFT OUTER JOIN mobile_lm.dlm_item CC
                ON AA.entity_id = CC.item_cd
                LEFT OUTER JOIN (
                    SELECT actor_account_id as target_account_id,actor_world as target_world,actor_id as target_id
                    ,actor_name as target_name,entity_id,time as selltime,data1_str,target_num1
                    FROM mobile_lm.lm_gamelog_logid
                    WHERE plogid = 2531
                    AND   plogdate >= DATE_FORMAT(DATE_ADD('[:DateLine]',-3),'yyyyMMdd')
                    AND   plogdate <= '[:DateNoLine]'
                    AND   actor_world != 90
                    AND   actor_session_id != 10011
                    AND   Log_Detail_Code = 2
                ) DD
                ON AA.target_account_id = DD.target_account_id  AND AA.entity_id = DD.entity_id AND AA.data1_str = DD.data1_str AND AA.target_num1 = DD.target_num1
                WHERE AA.plogid = 2536
                AND   AA.plogdate = '[:DateNoLine]'
                AND   AA.actor_world != 90
                AND   AA.actor_session_id != 10011
                AND   AA.Log_Detail_Code = 2
            ) tmp """
        mutiInsertSQLCode = """ 
            INSERT OVERWRITE TABLE rcenter.gama_bda_model_extract 
            PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='16509') 
            SELECT actor_account_id AS CommonData_1, actor_world AS CommonData_2, actor_id AS CommonData_3
            , actor_name AS CommonData_4, actor_class AS CommonData_5, actor_level AS CommonData_6
            , NULL AS CommonData_7, item_id AS CommonData_8, item_nm AS CommonData_9
            , item_group_nm AS CommonData_10, NULL AS CommonData_11, NULL AS CommonData_12
            , NULL AS CommonData_13, NULL AS CommonData_14, NULL AS CommonData_15
            , diam AS UniqueInt_1, item_num AS UniqueInt_2, NULL AS UniqueInt_3
            , NULL AS UniqueInt_4, NULL AS UniqueInt_5, NULL AS UniqueInt_6
            , NULL AS UniqueInt_7, NULL AS UniqueInt_8, NULL AS UniqueInt_9
            , NULL AS UniqueInt_10, entity_bless AS UniqueInt_11, entity_lv AS UniqueInt_12
            , entity_grade AS UniqueInt_13, entity_attribute AS UniqueInt_14, NULL AS UniqueInt_15
            , target_account_id AS UniqueStr_1, target_world AS UniqueStr_2, target_id AS UniqueStr_3
            , target_name AS UniqueStr_4, sellid AS UniqueStr_5, grade AS UniqueStr_6
            , NULL AS UniqueStr_7, NULL AS UniqueStr_8, NULL AS UniqueStr_9
            , item AS UniqueStr_10, NULL AS UniqueStr_11, NULL AS UniqueStr_12
            , NULL AS UniqueStr_13, NULL AS UniqueStr_14, NULL AS UniqueStr_15
            , NULL AS UniqueStr_16, NULL AS UniqueStr_17, NULL AS UniqueStr_18
            , NULL AS UniqueStr_19, NULL AS UniqueStr_20
            , item_price AS UniqueDbl_1, NULL AS UniqueDbl_2, NULL AS UniqueDbl_3
            , NULL AS UniqueDbl_4, NULL AS UniqueDbl_5, NULL AS UniqueDbl_6
            , NULL AS UniqueDbl_7, NULL AS UniqueDbl_8, NULL AS UniqueDbl_9
            , NULL AS UniqueDbl_10, NULL AS UniqueDbl_11, NULL AS UniqueDbl_12
            , NULL AS UniqueDbl_13, NULL AS UniqueDbl_14, NULL AS UniqueDbl_15
            , NULL AS UniqueDbl_16, NULL AS UniqueDbl_17, NULL AS UniqueDbl_18
            , NULL AS UniqueDbl_19, NULL AS UniqueDbl_20
            , buytime AS UniqueTime_1, selltime AS UniqueTime_2, NULL AS UniqueTime_3
            , NULL AS OtherStr_1, NULL AS OtherStr_2, NULL AS OtherStr_3
            , NULL AS OtherStr_4, NULL AS OtherStr_5, NULL AS OtherStr_6
            , NULL AS OtherStr_7, NULL AS OtherStr_8, NULL AS OtherStr_9
            , NULL AS OtherStr_10
            , ARRAY(NULL) AS UniqueArray_1, ARRAY(NULL) AS UniqueArray_2, NULL AS UniqueJson_1
            WHERE actor_world = '[:World]' """
        return "MutiInsert", [fromSQLCode, mutiInsertSQLCode]