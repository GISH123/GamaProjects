from sql.common.bureport.BUReport_1800 import BUReport_1800 as BUReport_1800_Common

class BUReport_1800(BUReport_1800_Common) :

    @classmethod
    def insert1802DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]", "[:DateNoLine]", "COMMON", "1802"]

    @classmethod
    def insert1806DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]","[:DateNoLine]","COMMON","1806"]