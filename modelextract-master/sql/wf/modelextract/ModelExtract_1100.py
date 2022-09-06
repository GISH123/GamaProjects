
class ModelExtract_1100() :

    @classmethod
    def insert1103DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
 -- Make [:GameName] ME 1103
INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='wf',dt='20220101',world='COMMON',tablenumber='1103')
SELECT 
	AAAA.viewer_id as CommonData_1
    , null as CommonData_2
    , AAAA.viewer_id as CommonData_3
    , BBBB.name as CommonData_4
    , AAAA.viewer_id as CommonData_5
    , AAAA.viewer_id as CommonData_6
    , null as CommonData_7
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
    , AAAA.login_time as UniqueTime_1
    , AAAA.logout_time as UniqueTime_2
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
(	SELECT 
		AAA.viewer_id	
		, AAA.login_time
	-- CASE WHEN 
		, CASE 
			WHEN MAX(BBB.battle_end_time) IS NOT NULL THEN MAX(BBB.battle_end_time)
			ELSE CAST(from_unixtime(unix_timestamp(AAA.login_time)+180) AS timestamp)
		END as logout_time
		, MAX(BBB.battle_end_time) AS end_time
	FROM (
		-- A 表 子查詢
		SELECT 
			viewer_id 
			, create_time as login_time
			, LEAD(create_time) OVER(PARTITION BY viewer_id ORDER BY create_time) as next_login_time
		FROM wf_all.pinball_log_2nd_log_login AA
		WHERE 1 = 1 
			AND AA.dt = '20220101'
	) AAA
	LEFT JOIN (
		-- B 表子查詢
		SELECT 
			AA.viewer_id
			, AA.play_id
			, AA.create_time as start_time
			, BB.create_time as end_time
			, BB.elapsed_time_ms
			, case 
				when BB.create_time is null then AA.create_time 
				else BB.create_time 
			  end as battle_end_time
		FROM wf_all.pinball_log_1st_log_multi_battle_quest_start AA
		INNER JOIN wf_all.pinball_log_1st_log_multi_battle_quest_finish BB ON 1 = 1 
			AND AA.play_id = BB.play_id
		WHERE 1 = 1 
			AND AA.dt = '20220101'
			AND BB.dt = '20220101'
	) BBB ON 1 = 1 
		AND AAA.viewer_id = BBB.viewer_id
	WHERE 1 = 1 
	-- AND OR AND OR 使用
		AND ( 1 != 1 
			OR ( 1 = 1 
				AND BBB.battle_end_time >= AAA.login_time
				AND BBB.battle_end_time <= AAA.next_login_time
			)
			OR BBB.battle_end_time is null 
		)
	GROUP bY 
	-- GROUP BY 使用
		AAA.viewer_id	
		, AAA.login_time
)
AAAA
LEFT JOIN (
	SELECT * 
	FROM wf_all.pinball1_user_info_00 AAA
	WHERE 1 = 1
		AND AAA.dt = '20220101'
	UNION ALL 
	SELECT * 
	FROM wf_all.pinball2_user_info_01 AAA
	WHERE 1 = 1
		AND AAA.dt = '20220101'
	UNION ALL
	SELECT * 
	FROM wf_all.pinball3_user_info_02 AAA
	WHERE 1 = 1
		AND AAA.dt = '20220101'
	UNION ALL
	SELECT * 
	FROM wf_all.pinball4_user_info_03 AAA
	WHERE 1 = 1
		AND AAA.dt = '20220101'
	UNION ALL
	SELECT * 
	FROM wf_all.pinball5_user_info_04 AAA
	WHERE 1 = 1
		AND AAA.dt = '20220101'
	UNION ALL
	SELECT * 
	FROM wf_all.pinball6_user_info_05 AAA
	WHERE 1 = 1
		AND AAA.dt = '20220101'
	UNION ALL
	SELECT * 
	FROM wf_all.pinball7_user_info_06 AAA
	WHERE 1 = 1
		AND AAA.dt = '20220101'
	UNION ALL
	SELECT * 
	FROM wf_all.pinball8_user_info_07 AAA
	WHERE 1 = 1
		AND AAA.dt = '20220101'
) BBBB ON 1 = 1 AND AAAA.viewer_id = BBBB.viewer_id; """
        return "OrderInsert", [orderInsertSQLCode1]