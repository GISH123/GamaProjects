
class ModelExtract_6000() :

    @classmethod
    def insert6001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
         -- Make [:GameName] ME 6001
 INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game= 'wf',dt='20220101',world='COMMON',tablenumber='6001')
SELECT  
	viewer_id as CommonData_1
    , null as CommonData_2
    , null as CommonData_3
    , null as CommonData_4
    , null as CommonData_5
    , null as CommonData_6
    , null as CommonData_7
    , null as CommonData_8
    , product_name as CommonData_9
    , null as CommonData_10
    , null as CommonData_11
    , null as CommonData_12
    , null as CommonData_13
    , null as CommonData_14
    , null as CommonData_15
    , 1 as UniqueInt_1
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
    , platform as UniqueStr_1
    , currency_iso as UniqueStr_2
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
    , payment_money as uniquedbl_1
    , price as uniquedbl_2
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
    , create_time as UniqueTime_1
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
	SELECT viewer_id, AA.platform, create_time, dt, product_name, price, currency_iso, payment_money
	FROM wf_extract.pinball_log_1st_log_user_payment AS AA
	INNER JOIN gtwpd.chingtien_wf_product_price AS BB
	ON AA.product_key = BB.product_id 
	AND AA.platform = BB.platform
	WHERE dt = '20220101' 
) AAA; """
        return "OrderInsert", [orderInsertSQLCode1]


    @classmethod
    def insert6002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
                 -- Make [:GameName] ME 16002
                   INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='wf',dt='20220101',world='COMMON',tablenumber='6002')
SELECT
	viewer_id as CommonData_1
	,null as CommonData_2
	,viewer_id as CommonData_3
	,null as CommonData_4
	,null as CommonData_5
	,null as CommonData_6
	,null as CommonData_7
	,'paid' as CommonData_8
	,'vmoney' as CommonData_9
	,platform as CommonData_10
	,null as CommonData_11
	,null as CommonData_12
	,null as CommonData_13
    , null as CommonData_14
    , null as CommonData_15
    , vmoney_diff as UniqueInt_1
    , 1 as UniqueInt_2
    , before_purchase_paid_vmoney as UniqueInt_3
    , after_purchase_paid_vmoney as UniqueInt_4
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
    , behavior as UniqueStr_1
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
    , create_time as UniqueTime_1
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
SELECT *, after_purchase_paid_vmoney - before_purchase_paid_vmoney AS vmoney_diff
FROM wf_extract.pinball_log_2nd_log_paid_vmoney AA
INNER JOIN gtwpd.chingtien_wf_action_number BB
ON AA.action_no = BB.action_code
WHERE dt = '20220101'
) AS AAA;
               """
        return "OrderInsert", [orderInsertSQLCode1]
