import time

class ModelExtract_15000() :

    @classmethod
    def insert15009DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 15009
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='15009')
            SELECT
                AA.CommonData_1 as CommonData_1
                , AA.CommonData_2 as CommonData_2
                , AA.CommonData_3 as CommonData_3
                , AA.CommonData_4 as CommonData_4
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
                , CASE WHEN CC.commondata_1 is null THEN 0 ELSE 1 END as UniqueInt_1
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
                , BB.friendid as UniqueStr_1
                , BB.friendname as UniqueStr_2
                , BB.friendgroup as UniqueStr_3
                , BB.flag as UniqueStr_4
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
                , BB.createdate as UniqueTime_1
                , BB.lastmodifieddate as UniqueTime_2
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
            FROM gtwpd.modelextract_modelextract AA
            INNER JOIN maple_all.gamedb_friend BB ON 1 = 1
                AND AA.commondata_3 = BB.characterid
                AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
                AND BB.world = '[:World]'
            LEFT JOIN gtwpd.modelextract_modelextract CC ON 1 = 1
                AND BB.friendid = CC.commondata_3
                AND CC.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND CC.world = '[:World]'
                AND CC.tablenumber = 11003
            WHERE 1 = 1
                AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                AND AA.world = '[:World]'
                AND AA.tablenumber = 11003 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert15109DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 5109
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='15109')
            SELECT
                null as CommonData_1
                , null as CommonData_2
                , AAA.characterid as CommonData_3
                , null as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , CASE WHEN MAX(AAA.guildid_i) = MAX(AAA.guildid_f) THEN MAX(AAA.guildid_f) ELSE NULL END as CommonData_8
                , CASE WHEN MAX(AAA.guildid_i) = MAX(AAA.guildid_f) THEN MAX(AAA.guildname_f) ELSE NULL END as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , CASE WHEN MAX(AAA.guildid_i) = MAX(AAA.guildid_f) THEN MAX(AAA.grade_f) ELSE NULL END as UniqueInt_1
                , CASE WHEN MAX(AAA.guildid_i) = MAX(AAA.guildid_f) THEN MAX(AAA.commitment_i) ELSE NULL END as UniqueInt_2
                , CASE WHEN MAX(AAA.guildid_i) = MAX(AAA.guildid_f) THEN MAX(AAA.commitment_f) ELSE NULL END as UniqueInt_3
                , CASE WHEN MAX(AAA.guildid_i) = MAX(AAA.guildid_f) THEN MAX(AAA.commitment_f)-MAX(AAA.commitment_i) ELSE NULL END as UniqueInt_4
                , CASE WHEN MAX(AAA.guildid_i) = MAX(AAA.guildid_f) THEN MAX(AAA.islogin) ELSE NULL END as UniqueInt_5
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
            FROM (
                SELECT
                    AA.characterid
                    , AA.guildid AS guildid_i
                    , BB.guildname AS guildname_i
                    , AA.grade AS grade_i
                    , AA.commitment AS commitment_i
                    , 0 AS guildid_f
                    , null AS guildname_f
                    , null AS grade_f
                    , 0 AS commitment_f
                    , 0 AS islogin
                FROM maple_extract.gamedb_guildmember AA
                INNER JOIN maple_extract.gamedb_guildinfo BB ON 1 = 1
                    AND AA.guildid = BB.guildid
                    AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',-1),'yyyyMMdd')
                    AND BB.world = '[:World]'
                WHERE 1 = 1
                    AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',-1),'yyyyMMdd')
                    AND AA.world = '[:World]'
                UNION ALL
                SELECT
                    AA.characterid
                    , 0 AS guildid_i
                    , null AS guildname_i
                    , null AS grade_i
                    , 0 AS commitment_i
                    , AA.guildid AS guildid_f
                    , BB.guildname AS guildname_f
                    , AA.grade AS grade_f
                    , AA.commitment AS commitment_f
                    , CASE WHEN CC.commondata_1 IS NULL THEN 0 ELSE 1 END AS islogin
                FROM maple_extract.gamedb_guildmember AA
                INNER JOIN maple_extract.gamedb_guildinfo BB ON 1 = 1
                    AND AA.guildid = BB.guildid
                    AND BB.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND BB.world = '[:World]'
                LEFT JOIN gtwpd.modelextract_modelextract CC ON 1 = 1
                    AND AA.characterid = CC.commondata_3
                    AND CC.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND CC.world = '[:World]'
                    AND CC.tablenumber = 11003
                WHERE 1 = 1
                    AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
                    AND AA.world = '[:World]'
            ) AAA
            GROUP BY
                AAA.characterid ;
        """
        return "OrderInsert", [orderInsertSQLCode1]





