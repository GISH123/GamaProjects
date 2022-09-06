import time

class ModelExtract_2000() :

    @classmethod
    def insert2002DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 2002
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='2002')
            SELECT distinct
                CC.CommonData_1 as CommonData_1
                , CC.CommonData_2  as CommonData_2
                , BB.characterid as CommonData_3
                , BB.charactername as CommonData_4
                , CC.CommonData_5 as CommonData_5
                , CC.CommonData_6 as CommonData_6
                , CASE WHEN CC.CommonData_5 is not null THEN 'TW' ELSE 'HK' END as CommonData_7
                , null as CommonData_8
                , null as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , CharacterLevel as UniqueInt_1
                , JobCode as UniqueInt_2
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
                , LastLoginIP as UniqueStr_1
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
                , LastLoginDate as UniqueTime_1
                , LastLogoutDate as UniqueTime_2
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
				    dt, CharacterID, CharacterLevel, LastLoginDate, LastLogoutDate, JobCode
                    , CONCAT(CAST(FLOOR(IF(LastLoginIP<0, LastLoginIP+4294967296, LastLoginIP)/16777216) AS STRING)
                    , '.', CAST(FLOOR(IF(LastLoginIP<0, LastLoginIP+4294967296, LastLoginIP)/65536%256) AS STRING)
                    , '.', CAST(FLOOR(IF(LastLoginIP<0, LastLoginIP+4294967296, LastLoginIP)/256%256) AS STRING)
                    , '.', '0') as LastLoginIP
				FROM tdn_extract.dnworld_characterstatus
            	WHERE dt = '[:DateNoLine]'
            ) AA
            LEFT JOIN (
                SELECT *
                FROM tdn_extract.dnmembership_characters
                WHERE dt = '[:DateNoLine]'
                    AND worldid = '[:World]'
            ) BB
            ON AA.characterid = BB.characterid
            LEFT JOIN (
            	SELECT *
            	FROM gtwpd.modelextract_modelextract
	            WHERE game = '[:GameName]'
	                AND dt = '[:DateNoLine]'
	                AND tablenumber = 11002
            ) CC
            ON BB.accountid = CC.CommonData_2 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]