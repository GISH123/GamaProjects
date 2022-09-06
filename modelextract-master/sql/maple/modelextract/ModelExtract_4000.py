import time

class ModelExtract_4000() :

    @classmethod
    def insert4001DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 4001
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='4001')
            SELECT
                null as CommonData_1
                , null as CommonData_2
                , AA.CommonData_3 as CommonData_3
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
                ----------------------------------------
                , count(*) as UniqueInt_1
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
                ----------------------------------------
                , CASE WHEN AA.UniqueStr_1 is null THEN '無類別' ELSE AA.UniqueStr_1 END as UniqueStr_1
                , null as UniqueStr_2
                , null as UniqueStr_3
                , null as UniqueStr_4
                , null as UniqueStr_5
                , null as UniqueStr_6
                , null as UniqueStr_7
                , null as UniqueStr_8
                , null as UniqueStr_9
                , null as UniqueStr_10
                , null as uniquestr_11
                , null as uniquestr_12
                , null as uniquestr_13
                , null as uniquestr_14
                , null as uniquestr_15
                , null as uniquestr_16
                , null as uniquestr_17
                , null as uniquestr_18
                , null as uniquestr_19
                , null as uniquestr_20
                ----------------------------------------
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
                ----------------------------------------
                , null as UniqueTime_1
                , null as UniqueTime_2
                , null as UniqueTime_3
                ----------------------------------------
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
                ----------------------------------------
                , array(null) as UniqueArray_1
                , array(null) as UniqueArray_2
                , null as UniqueJson_1
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game='[:GameName]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.world = '[:World]'
                AND AA.tablenumber = 4009
            GROUP BY
                AA.CommonData_3
                , CASE WHEN AA.UniqueStr_1 is null THEN '無類別' ELSE AA.UniqueStr_1 END  ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert4009DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 4009
           	INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='4009')
			SELECT
			    null as CommonData_1
			    , null as CommonData_2
			    , AAAA.characterid as CommonData_3
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
			    ----------------------------------------
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
			    ----------------------------------------
			    , BBBB.UniqueStr_2 as UniqueStr_1 --questtype
			    , AAAA.qrkey as UniqueStr_2
			    , BBBB.UniqueStr_3 as UniqueStr_3 --questname
			    , null as UniqueStr_4
			    , null as UniqueStr_5
			    , null as UniqueStr_6
			    , null as UniqueStr_7
			    , null as UniqueStr_8
			    , null as UniqueStr_9
			    , null as UniqueStr_10
			    , null as uniquestr_11
			    , null as uniquestr_12
			    , null as uniquestr_13
			    , null as uniquestr_14
			    , null as uniquestr_15
			    , null as uniquestr_16
			    , null as uniquestr_17
			    , null as uniquestr_18
			    , null as uniquestr_19
			    , null as uniquestr_20
			    ----------------------------------------
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
			    ----------------------------------------
			    , AAAA.finishtime as UniqueTime_1
			    , null as UniqueTime_2
			    , null as UniqueTime_3
			    ----------------------------------------
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
			    ----------------------------------------
			    , array(null) as UniqueArray_1
			    , array(null) as UniqueArray_2
			    , null as UniqueJson_1
			FROM maple_extract.gamedb_questcompletenew AAAA
			LEFT JOIN (
			    SELECT
			        *
			    FROM gtwpd.modelextract_modelextract BBB 
			    WHERE 1 = 1
			        AND BBB.dt in (
			            SELECT max(dt)
			            FROM gtwpd.modelextract_modelextract BB
			            WHERE 1 = 1
			                AND BB.game = '[:GameName]'
			                AND BB.tablenumber = '14051'
			        )
			        AND BBB.game = '[:GameName]'
			        AND BBB.tablenumber = '14051'
			) BBBB ON 1 = 1
			    AND AAAA.qrkey = BBBB.UniqueStr_1
			WHERE 1 = 1
			    AND AAAA.dt = '[:DateNoLine]'
			    AND AAAA.world = '[:World]' ;
        """
        return "OrderInsert", [orderInsertSQLCode1]


