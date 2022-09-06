DROP TABLE IF EXISTS gtwpd.temp_asn_new;
CREATE EXTERNAL TABLE gtwpd.temp_asn_new
(
   	ip_from bigint
	, ip_end bigint
	, c_idr	string
	, asn_code	string
    , asn_name	string
	, ip_from_str	string
    , ip_end_str	string
	, ip_cnt string
    , ip_sec string
	, ip_sec1 string
	, ip_sec2 string
	, ip_sec3 string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/GTW_PD/DB/TEMP/temp_ipmap_new/asp/';
WITH tb AS
(
SELECT
   	ip_from
	, ip_end
    , ip_sec 
	, ip_sec1 
	, ip_sec2 
	, ip_sec3 
	, asn_code
    , asn_name
FROM
	gtwpd.temp_asn_new
)
INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='COMMON',dt='[:DateNoLine]',world='COMMON',tablenumber='11862')
SELECT
	null as CommonData_1
    , null  as CommonData_2
    , null as CommonData_3
    , null as CommonData_4
    , null as CommonData_5
    , null as CommonData_6
    , null as CommonData_7
    , null as CommonData_8
    , null as CommonData_9
    , null as CommonData_10
    , null as CommonData_11
    , null as CommonData_12
    , null as CommonData_13
    , null as CommonData_14
    , null as CommonData_15
    , ip_from as UniqueInt_1
    , ip_end as UniqueInt_2
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
    , ip_sec  as UniqueStr_1
    , null  as UniqueStr_2
    , asn_code  as UniqueStr_3
    , asn_name  as UniqueStr_4
    , null  as UniqueStr_5
    , null as UniqueStr_6
    , null as UniqueStr_7
    , null as UniqueStr_8
    , null as UniqueStr_9
    , null as UniqueStr_10
    , ip_sec1 as UniqueStr_11
    , ip_sec2 as UniqueStr_12
    , ip_sec3 as UniqueStr_13
    , null as UniqueStr_14
    , null as UniqueStr_15
    , null as UniqueStr_16
    , null as UniqueStr_17
    , null as UniqueStr_18
    , null as UniqueStr_19
    , null as UniqueStr_20
    , null as UniqueDBL_1
    , null as UniqueDBL_2
    , null as UniqueDBL_3
    , null as UniqueDBL_4
    , null as UniqueDBL_5
    , null as UniqueDBL_6
    , null as UniqueDBL_7
    , null as UniqueDBL_8
    , null as UniqueDBL_9
    , null as UniqueDBL_10
    , null as UniqueDBL_11
    , null as UniqueDBL_12
    , null as UniqueDBL_13
    , null as UniqueDBL_14
    , null as UniqueDBL_15
    , null as UniqueDBL_16
    , null as UniqueDBL_17
    , null as UniqueDBL_18
    , null as UniqueDBL_19
    , null as UniqueDBL_20
    , null as UniqueTime_1
    , null as UniqueTime_2
    , null as UniqueTime_3
    , null as OtherStr_1
    , null as OtherStr_2
    , null as OtherStr_3
    , null as OtherStr_4
    , null as OtherStr_5
    , null as OtherStr_6
    , null as OtherStr_7
    , null as OtherStr_8
    , null as OtherStr_9
    , null as OtherStr_10
    , array(null) as UniqueArray_1
    , array(null) as UniqueArray_2
    , null as UniqueJson_1
FROM tb
;