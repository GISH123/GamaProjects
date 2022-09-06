from sql.common.bureport.BUReport_1800 import BUReport_1800 as BUReport_1800_Common

class BUReport_1800(BUReport_1800_Common) :

    @classmethod
    def insert1804DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]", "[:DateNoLine]", "COMMON", "1804"]

    @classmethod
    def insert1806DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]","[:DateNoLine]","COMMON","1806"]