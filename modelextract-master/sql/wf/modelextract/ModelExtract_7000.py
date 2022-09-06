
class ModelExtract_7000() :

    @classmethod
    def insert17001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
                 -- Make [:GameName] ME 17001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:wf]',dt='[:DateNoLine]',world='COMMON',tablenumber='17001')
       SELECT
	viewer_id as CommonData_1
    , character_id as CommonData_2
    , add_type as CommonData_3
    , add_reason as CommonData_4
    , action_no as CommonData_5
    , behavior as CommonData_6
    , create_time CommonData_7
    , null as CommonData_8
    , null as CommonData_9
    , null as CommonData_10
    , null as CommonData_11
    , null as CommonData_12
    , null as CommonData_13
    , null as CommonData_14
    , null as CommonData_15
    , null as UniqueInt_1
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
FROM
(
SELECT viewer_id, character_id, add_type, 
CASE
WHEN add_type = 0 THEN '歸還'
WHEN add_type = 1 THEN '追加'
WHEN add_type = 2 THEN '因為重複而堆疊化'
ELSE 'ww' END AS add_reason,
AA.action_no, behavior, create_time, dt 
FROM wf_extract.pinball_log_1st_log_character AA
INNER JOIN gtwpd.chingtien_wf_action_number BB
ON AA.action_no = BB.action_code
WHERE dt = '[:DateNoLine]'
) AAA; """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert17002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
                 -- Make [:GameName] ME 17002
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:wf]',dt='[:DateNoLine]',world='COMMON',tablenumber='17002')
SELECT
	viewer_id as CommonData_1
    , character_id as CommonData_2
    , add_type as CommonData_3
    , add_reason as CommonData_4
    , action_no as CommonData_5
    , behavior as CommonData_6
    , create_time CommonData_7
    , null as CommonData_8
    , null as CommonData_9
    , null as CommonData_10
    , null as CommonData_11
    , null as CommonData_12
    , null as CommonData_13
    , null as CommonData_14
    , null as CommonData_15
    , null as UniqueInt_1
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
FROM
(
SELECT viewer_id, character_id, add_type, 
CASE
WHEN add_type = 0 THEN '歸還'
WHEN add_type = 1 THEN '追加'
WHEN add_type = 2 THEN '因為重複而堆疊化'
ELSE 'ww' END AS add_reason,
AA.action_no, behavior, create_time, dt 
FROM wf_extract.pinball_log_1st_log_character AA
INNER JOIN gtwpd.chingtien_wf_action_number BB
ON AA.action_no = BB.action_code
WHERE dt = '[:DateNoLine]'
) AAA;"""
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert17003DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
        -- Make [:GameName] ME 17003
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:wf]',dt='[:DateNoLine]',world='COMMON',tablenumber='17003')
           SELECT
	viewer_id as CommonData_1
    , before_rank as CommonData_2
    , after_rank as CommonData_3
    , rank_diff as CommonData_4
    , create_time as CommonData_5
    , null as CommonData_6
    , null CommonData_7
    , null as CommonData_8
    , null as CommonData_9
    , null as CommonData_10
    , null as CommonData_11
    , null as CommonData_12
    , null as CommonData_13
    , null as CommonData_14
    , null as CommonData_15
    , null as UniqueInt_1
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
FROM
(SELECT viewer_id, before_rank, after_rank, after_rank - before_rank AS rank_diff, create_time, dt FROM wf_extract.pinball_log_1st_log_user_rankup
WHERE dt = '[:DateNoLine]'
) AAA; """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert17004DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
                 -- Make [:GameName] ME 17004

INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='wf',dt='[:DateNoLine]',world='COMMON',tablenumber='17004')
    SELECT
	viewer_id as CommonData_1
    , gacha_type as CommonData_2
    , gacha_count as CommonData_3
    , create_time as CommonData_4
    , null as CommonData_5
    , null as CommonData_6
    , null CommonData_7
    , null as CommonData_8
    , null as CommonData_9
    , null as CommonData_10
    , null as CommonData_11
    , null as CommonData_12
    , null as CommonData_13
    , null as CommonData_14
    , null as CommonData_15
    , null as UniqueInt_1
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
FROM
(SELECT viewer_id, gacha_type, 
CASE 
WHEN gacha_type = 1 THEN 1
WHEN gacha_type = 2 THEN 10 END AS gacha_count,
create_time, dt
FROM wf_extract.pinball_log_2nd_log_premium_gacha
WHERE dt = '[:DateNoLine]') AAA;
"""
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert17005DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
        -- Make [:GameName] ME 17005
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:wf]',dt='[:DateNoLine]',world='COMMON',tablenumber='17005')
            SELECT
	viewer_id as CommonData_1
    , before_stamina as CommonData_2
    , after_stamina as CommonData_3
    , diff_stamina as CommonData_4
    , change_type as CommonData_5
    , change_reason as CommonData_6
    , create_time CommonData_7
    , null as CommonData_8
    , null as CommonData_9
    , null as CommonData_10
    , null as CommonData_11
    , null as CommonData_12
    , null as CommonData_13
    , null as CommonData_14
    , null as CommonData_15
    , null as UniqueInt_1
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
FROM
(SELECT viewer_id, before_stamina, after_stamina, after_stamina - before_stamina AS diff_stamina, change_type, 
CASE 
WHEN change_type = 1 THEN '星導石'
WHEN change_type = 2 THEN '道具'
WHEN change_type = 3 THEN '時間'
WHEN change_type = 101 THEN '任務'
ELSE '' END AS change_reason,S
create_time, dt FROM wf_extract.pinball_log_2nd_log_stamina
WHERE dt = '[:DateNoLine]') AAA; """
        return "OrderInsert", [orderInsertSQLCode1]