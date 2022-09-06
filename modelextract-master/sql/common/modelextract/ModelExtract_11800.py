import time


class ModelExtract_11800 :

    @classmethod
    def insert11853DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='COMMON',dt='[:DateNoLine]',world='COMMON',tablenumber='11853')
            SELECT
                commondata_1
                , commondata_2
                , commondata_3
                , commondata_4
                , commondata_5
                , commondata_6
                , commondata_7
                , commondata_8
                , commondata_9
                , commondata_10
                , commondata_11
                , commondata_12
                , commondata_13
                , commondata_14
                , commondata_15
                , uniqueint_1
                , uniqueint_2
                , uniqueint_3
                , uniqueint_4
                , uniqueint_5
                , uniqueint_6
                , uniqueint_7
                , uniqueint_8
                , uniqueint_9
                , uniqueint_10
                , uniqueint_11
                , uniqueint_12
                , uniqueint_13
                , uniqueint_14
                , uniqueint_15
                , uniquestr_1
                , uniquestr_2
                , uniquestr_3
                , uniquestr_4
                , uniquestr_5
                , uniquestr_6
                , uniquestr_7
                , uniquestr_8
                , uniquestr_9
                , uniquestr_10
                , uniquestr_11 
                , uniquestr_12 
                , uniquestr_13 
                , uniquestr_14 
                , uniquestr_15 
                , uniquestr_16 
                , uniquestr_17 
                , uniquestr_18 
                , uniquestr_19 
                , uniquestr_20 
                , uniquedbl_1 
                , uniquedbl_2 
                , uniquedbl_3 
                , uniquedbl_4 
                , uniquedbl_5 
                , uniquedbl_6 
                , uniquedbl_7 
                , uniquedbl_8 
                , uniquedbl_9 
                , uniquedbl_10 
                , uniquedbl_11 
                , uniquedbl_12 
                , uniquedbl_13 
                , uniquedbl_14 
                , uniquedbl_15 
                , uniquedbl_16 
                , uniquedbl_17 
                , uniquedbl_18 
                , uniquedbl_19 
                , uniquedbl_20
                , uniquetime_1
                , uniquetime_2
                , uniquetime_3
                , otherstr_1 
                , otherstr_2 
                , otherstr_3 
                , otherstr_4 
                , otherstr_5 
                , otherstr_6 
                , otherstr_7 
                , otherstr_8 
                , otherstr_9 
                , otherstr_10
                , UniqueArray_1
                , UniqueArray_2
                , uniquejson_1
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game='COMMON'
                AND AA.world='COMMON'
                AND AA.tablenumber='11853'
                AND AA.dt = '[:DateNoLine]'
        """
        return "OrderInsert", [orderInsertSQLCode1]


    @classmethod
    def insert11854DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='COMMON',dt='[:DateNoLine]',world='COMMON',tablenumber='11854')
            SELECT
                AA.commondata_1
                , AA.commondata_2
                , AA.commondata_3
                , AA.commondata_4
                , AA.commondata_5
                , AA.commondata_6
                , AA.commondata_7
                , AA.commondata_8
                , AA.commondata_9
                , AA.commondata_10
                , AA.commondata_11
                , AA.commondata_12
                , AA.commondata_13
                , AA.commondata_14
                , AA.commondata_15
                , AA.uniqueint_1
                , AA.uniqueint_2
                , AA.uniqueint_3
                , AA.uniqueint_4
                , AA.uniqueint_5
                , AA.uniqueint_6
                , AA.uniqueint_7
                , AA.uniqueint_8
                , AA.uniqueint_9
                , AA.uniqueint_10
                , AA.uniqueint_11
                , AA.uniqueint_12
                , AA.uniqueint_13
                , AA.uniqueint_14
                , AA.uniqueint_15
                , AA.uniquestr_1
                , AA.uniquestr_2
                , AA.uniquestr_3
                , AA.uniquestr_4
                , AA.uniquestr_5
                , AA.uniquestr_6
                , AA.uniquestr_7
                , AA.uniquestr_8
                , AA.uniquestr_9
                , AA.uniquestr_10
                , AA.uniquestr_11 
                , AA.uniquestr_12 
                , AA.uniquestr_13 
                , AA.uniquestr_14 
                , AA.uniquestr_15 
                , AA.uniquestr_16 
                , AA.uniquestr_17 
                , AA.uniquestr_18 
                , AA.uniquestr_19 
                , AA.uniquestr_20 
                , AA.uniquedbl_1 
                , AA.uniquedbl_2 
                , AA.uniquedbl_3 
                , AA.uniquedbl_4 
                , AA.uniquedbl_5 
                , AA.uniquedbl_6 
                , AA.uniquedbl_7 
                , AA.uniquedbl_8 
                , AA.uniquedbl_9 
                , AA.uniquedbl_10 
                , AA.uniquedbl_11 
                , AA.uniquedbl_12 
                , AA.uniquedbl_13 
                , AA.uniquedbl_14 
                , AA.uniquedbl_15 
                , AA.uniquedbl_16 
                , AA.uniquedbl_17 
                , AA.uniquedbl_18 
                , AA.uniquedbl_19 
                , AA.uniquedbl_20
                , AA.uniquetime_1
                , AA.uniquetime_2
                , AA.uniquetime_3
                , AA.otherstr_1 
                , AA.otherstr_2 
                , AA.otherstr_3 
                , AA.otherstr_4 
                , AA.otherstr_5 
                , AA.otherstr_6 
                , AA.otherstr_7 
                , AA.otherstr_8 
                , AA.otherstr_9 
                , AA.otherstr_10
                , AA.UniqueArray_1
                , AA.UniqueArray_2
                , AA.uniquejson_1
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = 'COMMON'
                AND AA.world = 'COMMON'
                AND AA.tablenumber = '11851'
                AND AA.dt in (
                    SELECT max(dt) 
                    FROM gtwpd.modelextract_modelextract 
                    WHERE tablenumber = '11854'
                )	
                AND AA.commondata_7 IN ('TW','HK')
            UNION ALL 
            SELECT
                AA.commondata_1
                , AA.commondata_2
                , AA.commondata_3
                , AA.commondata_4
                , AA.commondata_5
                , AA.commondata_6
                , AA.commondata_7
                , AA.commondata_8
                , AA.commondata_9
                , AA.commondata_10
                , AA.commondata_11
                , AA.commondata_12
                , AA.commondata_13
                , AA.commondata_14
                , AA.commondata_15
                , AA.uniqueint_1
                , AA.uniqueint_2
                , AA.uniqueint_3
                , AA.uniqueint_4
                , AA.uniqueint_5
                , AA.uniqueint_6
                , AA.uniqueint_7
                , AA.uniqueint_8
                , AA.uniqueint_9
                , AA.uniqueint_10
                , AA.uniqueint_11
                , AA.uniqueint_12
                , AA.uniqueint_13
                , AA.uniqueint_14
                , AA.uniqueint_15
                , AA.uniquestr_1
                , AA.uniquestr_2
                , AA.uniquestr_3
                , AA.uniquestr_4
                , AA.uniquestr_5
                , AA.uniquestr_6
                , AA.uniquestr_7
                , AA.uniquestr_8
                , AA.uniquestr_9
                , AA.uniquestr_10
                , AA.uniquestr_11 
                , AA.uniquestr_12 
                , AA.uniquestr_13 
                , AA.uniquestr_14 
                , AA.uniquestr_15 
                , AA.uniquestr_16 
                , AA.uniquestr_17 
                , AA.uniquestr_18 
                , AA.uniquestr_19 
                , AA.uniquestr_20 
                , AA.uniquedbl_1 
                , AA.uniquedbl_2 
                , AA.uniquedbl_3 
                , AA.uniquedbl_4 
                , AA.uniquedbl_5 
                , AA.uniquedbl_6 
                , AA.uniquedbl_7 
                , AA.uniquedbl_8 
                , AA.uniquedbl_9 
                , AA.uniquedbl_10 
                , AA.uniquedbl_11 
                , AA.uniquedbl_12 
                , AA.uniquedbl_13 
                , AA.uniquedbl_14 
                , AA.uniquedbl_15 
                , AA.uniquedbl_16 
                , AA.uniquedbl_17 
                , AA.uniquedbl_18 
                , AA.uniquedbl_19 
                , AA.uniquedbl_20
                , AA.uniquetime_1
                , AA.uniquetime_2
                , AA.uniquetime_3
                , AA.otherstr_1 
                , AA.otherstr_2 
                , AA.otherstr_3 
                , AA.otherstr_4 
                , AA.otherstr_5 
                , AA.otherstr_6 
                , AA.otherstr_7 
                , AA.otherstr_8 
                , AA.otherstr_9 
                , AA.otherstr_10
                , AA.UniqueArray_1
                , AA.UniqueArray_2
                , AA.uniquejson_1
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = 'COMMON'
                AND AA.world = 'COMMON'
                AND AA.tablenumber = '11853'
                AND AA.dt in (
                    SELECT max(dt) 
                    FROM gtwpd.modelextract_modelextract 
                    WHERE tablenumber = '11853'
                )	
                AND AA.commondata_7 NOT IN ('TW','HK')
        """
        return "OrderInsert", [orderInsertSQLCode1]