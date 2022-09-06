import time


class ModelExtract_10000 :

    @classmethod
    def insert11001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ModelData 11001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='COMMON',tablenumber='11001')
            SELECT 
                T1.serviceaccountid as CommonData_1
                , T1.mainaccountid  as CommonData_2
                , T2.openid as CommonData_3
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
        FROM
            (SELECT 
                lower(serviceaccountid) as serviceaccountid,
                MAX(lower(mainaccount)) as mainaccountid
             FROM bf_extract.beanfundb_d_serviceaccount_f
             WHERE 1 = 1
             AND servicecode = '300148' 
             AND ('[:DateNoLine]' < '20200701' AND dt = '20200701')
             OR ('[:DateNoLine]' >='20200701' AND dt >= '[:DateNoLine]' AND dt <= DATE_FORMAT(DATE_ADD('[:DateLine]', 2),'yyyyMMdd'))
             GROUP BY lower(serviceaccountid)
            ) T1
        LEFT OUTER JOIN
            (SELECT lower(mainaccountid) as mainaccountid,
                    max(globalsn) as openid
             FROM bf_extract.beanfundb_bfapp_mainaccount
             WHERE 1 = 1
             AND ('[:DateNoLine]' < '20200701' AND dt = '20200701')
             OR ('[:DateNoLine]' >='20200701' AND dt >= '[:DateNoLine]' AND dt <= DATE_FORMAT(DATE_ADD('[:DateLine]', 2),'yyyyMMdd'))
             GROUP BY lower(mainaccountid)
            ) T2
        ON T1.mainaccountid = T2.mainaccountid;
        """
        return "OrderInsert", [orderInsertSQLCode1]
