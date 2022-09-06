import time
from sql.common.modelextract.ModelExtract_1100 import ModelExtract_1100 as ModelExtract_1100_Common


class ModelExtract_1100(ModelExtract_1100_Common):

    @classmethod
    def insert1102DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelExtract 1102
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1102')
            SELECT
                   T1.account_id as CommonData_1
                   , T1.account_id as CommonData_2
                   , null as CommonData_3
                   , null as CommonData_4
                   , T2.mainaccountid as CommonData_5
                   , T2.openid as CommonData_6
                   , CASE WHEN T2.mainaccountid is not null then 'TW' else 'HK' end as CommonData_7
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
                   , CASE WHEN T1.login_time <= '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00' ELSE T1.login_time END as UniqueTime_1
                   , CASE WHEN T1.logout_time >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59' ELSE T1.logout_time END as UniqueTime_2
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
                   , array(null) as UniqueArray_01
                   , array(null) as UniqueArray_02
                   , null as UniqueJson_01
            FROM
                (SELECT lower(account_id) as account_id,
                        login_time,
                        logout_time
                 FROM cso_extract.init_onlinetime
                 WHERE 1 = 1
                 AND dt = '[:DateNoLine]'
                ) T1
            LEFT OUTER JOIN
                (SELECT CommonData_1 as serviceaccountid,
                        CommonData_2 as mainaccountid,
                        CommonData_3 as openid
                 FROM gtwpd.modelextract_modelextract
                 WHERE 1 = 1
                 AND dt = '[:DateNoLine]'
                 AND tablenumber = '11001'
                 AND game = 'cso'
                 AND world = 'COMMON'
                ) T2
            ON T1.account_id = T2.serviceaccountid ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert1103DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
                -- Make [:GameName] ModelExtract 1103
                INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='1103')
                SELECT
                   T1.account_id as CommonData_1
                   , T1.account_id as CommonData_2
                   , T1.account_id as CommonData_3
                   , T4.charactername as CommonData_4
                   , T2.mainaccountid as CommonData_5
                   , T2.openid as CommonData_6
                   , CASE WHEN T2.mainaccountid is not null then 'TW' else 'HK' end as CommonData_7
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
                   , CASE WHEN T1.login_time <= '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00' ELSE T1.login_time END as UniqueTime_1
                   , CASE WHEN T1.logout_time >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59' ELSE T1.logout_time END as UniqueTime_2
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
                   , array(null) as UniqueArray_01
                   , array(null) as UniqueArray_02
                   , null as UniqueJson_01
                    FROM
                (SELECT lower(account_id) as account_id,
                        login_time,
                        logout_time
                 FROM cso_extract.init_onlinetime
                 WHERE 1 = 1
                 AND dt = '[:DateNoLine]'
                ) T1
            LEFT OUTER JOIN
                (SELECT CommonData_1 as serviceaccountid,
                        CommonData_2 as mainaccountid,
                        CommonData_3 as openid
                 FROM gtwpd.modelextract_modelextract
                 WHERE 1 = 1
                 AND dt = '[:DateNoLine]'
                 AND tablenumber = '11001'
                 AND game = 'cso'
                 AND world = 'COMMON'
                ) T2
            ON T1.account_id = T2.serviceaccountid
            LEFT OUTER JOIN
                (SELECT DISTINCT
                        lower(id) as account_id,
                        nickname as charactername,
                        createtime
                 FROM cso_extract.cso_character
                 WHERE 1 = 1
                 AND dt >= date_format(DATE_ADD('[:DateLine]', 0), 'yyyyMMdd')
                 AND dt <= date_format(DATE_ADD('[:DateLine]', 3), 'yyyyMMdd')
                 AND nexonsn IS NOT NULL
                 AND updatetime IS NOT NULL
                ) T4
            ON T1.account_id = T4.account_id  ;
                """
        return "OrderInsert", [orderInsertSQLCode1]

