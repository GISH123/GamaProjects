import time
from sql.common.modelextract.ModelExtract_6000 import ModelExtract_6000 as ModelExtract_6000_Common

class ModelExtract_6000(ModelExtract_6000_Common) :

    @classmethod
    def insert6019DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6019')
            SELECT DISTINCT
                lower(BB.accountname) as CommonData_1
                , BB.accountid as CommonData_2
                , AA.characterid as CommonData_3
                , BB.charactername as CommonData_4
                , game_accountid as CommonData_5
                , global_accountid as CommonData_6
                , CASE WHEN EE.game_accountid is not null THEN 'TW' ELSE 'HK' END as CommonData_7
                , CC.productid as CommonData_8
                , DD.productname as CommonData_9
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
                , 'bf point' as UniqueStr_1 --as newbeanfunbindaccount
                , null as UniqueStr_2 --as mainaccountid
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
                , CC.listprice as uniquedbl_1
                , CC.listprice as uniquedbl_2 
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
                , AA.orderdate as UniqueTime_1
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
                , array(null) as UniqueArray_01
                , array(null) as UniqueArray_02
                , null as UniqueJson_1
            FROM (
                SELECT *
                FROM tdn_extract.dnstaging_PurchaseOrders AA
                WHERE AA.orderdate >= DATE_ADD(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('[:DateNoLine]', 'yyyyMMdd'))), 0)
                    AND AA.orderdate < DATE_ADD(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('[:DateNoLine]', 'yyyyMMdd'))), 1)
                    AND AA.paymethodcode = '2'
                    AND AA.orderstatuscode = '2'
            ) AA
            
            INNER JOIN tdn_extract.dnworld_characters BB
            ON AA.characterid = BB.characterid
            AND BB.dt = '[:DateNoLine]'
            AND BB.world = '[:World]'
            
            LEFT JOIN tdn_extract.dnstaging_purchaseorderdetails CC 
            ON AA.purchaseorderid = CC.purchaseorderid
            AND CC.dt = '[:DateNoLine]'
            
            LEFT JOIN tdn_extract.dnstaging_dimproducts DD
            ON CC.productid = DD.productid
            AND DD.dt = '[:DateNoLine]'
            
            LEFT JOIN (
                SELECT 
                    commondata_2 as game_accountid
                    , commondata_5 as gash_accountid
                    , commondata_6 as global_accountid
                FROM gtwpd.modelextract_modelextract 
                WHERE tablenumber = '11002'
                AND dt = '[:DateNoLine]'
            ) EE 
            ON BB.accountid = EE.game_accountid;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert6509DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 6509 交易所(買)
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6509')
            SELECT DISTINCT
                FF.CommonData_1 as CommonData_1
                , BB.accountid as CommonData_2
                , AA.buyercharacterid as CommonData_3
                , BB.charactername as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , DD.itemid as CommonData_8
                , DD.itemname as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , AA.price as UniqueInt_1
                , AA.itemcount as UniqueInt_2
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
                , CC.accountid as UniqueStr_2
                , AA.characterid as UniqueStr_3
                , CC.charactername as UniqueStr_4
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
                , AA.tradedate as UniqueTime_1
                , EE.registerdate as UniqueTime_2
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
            	SELECT *
            	FROM tdn_extract.dnstaging_tradesuccesslogs
	            WHERE dt = '[:DateNoLine]'
	                AND worldid = '[:World]'
            ) AA
            
            LEFT JOIN (
                SELECT *
                FROM tdn_extract.dnmembership_characters
                WHERE dt = '[:DateNoLine]'
                    AND worldid = '[:World]'
            ) BB
            ON AA.BuyerCharacterID = BB.characterid
            
            LEFT JOIN (
                SELECT *
                FROM tdn_extract.dnmembership_characters
                WHERE dt = '[:DateNoLine]'
                    AND worldid = '[:World]'
            ) CC
            ON AA.characterid = CC.characterid
            
            LEFT JOIN (
                SELECT *
                FROM tdn_extract.dnstaging_dimitems
                WHERE dt = '[:DateNoLine]'
            ) DD
            ON AA.itemid = DD.itemid
            
            INNER JOIN (
                SELECT tradeid, registerdate
                FROM tdn_extract.dnstaging_traderegisterlogs
                WHERE dt = '[:DateNoLine]'
                    AND worldid = '[:World]'
            ) EE
            ON AA.tradeid = EE.tradeid
            
            LEFT JOIN (
            	SELECT CommonData_1, CommonData_2
            	FROM gtwpd.modelextract_modelextract
	            WHERE game = '[:GameName]'
	                AND dt = '[:DateNoLine]'
	                AND tablenumber = 11002
            ) FF
            ON BB.accountid = FF.CommonData_2 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]

    @classmethod
    def insert6609DataSQL(self, makeInfo):
        orderInsertSQLCode1 = """
            -- Make [:GameName] ME 6609 一對一(收)
            INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='[:GameName]',dt='[:DateNoLine]',world='[:World]',tablenumber='6609')
            SELECT DISTINCT
                FF.CommonData_1 as CommonData_1
                , BB.accountid as CommonData_2
                , AA.ownercharacterid as CommonData_3
                , BB.charactername as CommonData_4
                , null as CommonData_5
                , null as CommonData_6
                , null as CommonData_7
                , DD.itemid as CommonData_8
                , DD.itemname as CommonData_9
                , null as CommonData_10
                , null as CommonData_11
                , null as CommonData_12
                , null as CommonData_13
                , null as CommonData_14
                , null as CommonData_15
                , null as UniqueInt_1
                , AA.itemcount as UniqueInt_2
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
                , CC.accountid as UniqueStr_2
                , AA.ownercharacteridbefore as UniqueStr_3
                , CC.charactername as UniqueStr_4
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
                , AA.LogDate as UniqueTime_1
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
            	SELECT *
            	FROM tdn_extract.dnstaging_itemlogs
	            WHERE dt = '[:DateNoLine]'
	                AND worldid = '[:World]'
	                AND itemlogcode = 29
            ) AA
            
            LEFT JOIN (
                SELECT *
                FROM tdn_extract.dnmembership_characters
                WHERE dt = '[:DateNoLine]'
                    AND worldid = '[:World]'
            ) BB
            ON AA.ownercharacterid = BB.characterid
            
            LEFT JOIN (
                SELECT *
                FROM tdn_extract.dnmembership_characters
                WHERE dt = '[:DateNoLine]'
                    AND worldid = '[:World]'
            ) CC
            ON AA.ownercharacteridbefore = CC.characterid
            
            LEFT JOIN (
                SELECT *
                FROM tdn_extract.dnstaging_dimitems
                WHERE dt = '[:DateNoLine]'
            ) DD
            ON AA.itemid = DD.itemid
            
            LEFT JOIN (
            	SELECT CommonData_1, CommonData_2
            	FROM gtwpd.modelextract_modelextract
	            WHERE game = '[:GameName]'
	                AND dt = '[:DateNoLine]'
	                AND tablenumber = 11002
            ) FF
            ON BB.accountid = FF.CommonData_2 ;
        """
        return "OrderInsert", [orderInsertSQLCode1]