import time


class ModelExtract_1800 :

    @classmethod
    def insert1804DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelExtract 1804
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='1804')
            SELECT
                   T1.account_id as CommonData_1
                   , T1.account_id as CommonData_2
                   , null as CommonData_3
	               , null as CommonData_4
	               , T4.mainaccountid as CommonData_5
	               , T4.openid as CommonData_6
	               , CASE WHEN T4.mainaccountid is not null then 'TW' else 'HK' end as CommonData_7
                   , null as CommonData_8
                   , null as CommonData_9
                   , null as CommonData_10
                   , null as CommonData_11
                   , null as CommonData_12
                   , null as CommonData_13
                   , null as CommonData_14
                   , null as CommonData_15
                   , null as UniqueInt_1
                   , T1.logincnt as UniqueInt_2
                   , T2.ipcnt as UniqueInt_3
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
                   , T1.IP as UniqueStr_1
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
                   , accountarray as UniqueArray_01
                   , array(null) as UniqueArray_02
                   , null as UniqueJson_01
            FROM
                (SELECT lower(useruid) as account_id,
                        IP,
                        COUNT(1) AS logincnt
                 FROM els_extract.init_onlinetimebyip
                 WHERE 1 = 1
                 AND dt = '[:DateNoLine]'
                 GROUP BY lower(useruid),
                          IP
                ) T1
            LEFT OUTER JOIN
                (SELECT ip,
                        COUNT(DISTINCT useruid) AS ipcnt,
                        collect_set(lower(useruid)) AS accountarray
                 FROM els_extract.init_onlinetimebyip
                 WHERE 1 = 1
                 AND dt = '[:DateNoLine]'
                 GROUP BY ip
                ) T2
            ON T1.ip = T2.ip
            LEFT OUTER JOIN
                (SELECT lower(useruid) as useruid,
                        lower(userid) as serviceid,
                        regdate as account_createtime
                 FROM els_extract.account_muser
                 WHERE 1 = 1
                 AND dt = '[:DateNoLine]'
                ) T3
            ON T1.account_id = T3.useruid
            LEFT OUTER JOIN
                (SELECT CommonData_1 as serviceaccountid,
                        CommonData_2 as mainaccountid,
                        CommonData_3 as openid
                 FROM gtwpd.modelextract_modelextract
                 WHERE 1 = 1
                 AND dt = '[:DateNoLine]'
                 AND tablenumber = '11001'
                 AND game = 'els'
                 AND world = 'COMMON'
                ) T4
            ON T3.serviceid = T4.serviceaccountid ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

