import time
from sql.common.bureport.BUReport_1100 import BUReport_1100 as BUReport_1100_Common

class BUReport_6000() :

    @classmethod
    def insert6001DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]","[:DateNoLine]","COMMON","6001"]

    @classmethod
    def insert6002DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]", "[:DateNoLine]", "COMMON", "6002"]

    @classmethod
    def insert6011DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]", "[:DateNoLine]", "[:World]", "6011"]

    @classmethod
    def insert6012DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]", "[:DateNoLine]", "[:World]", "6012"]

    @classmethod
    def insert6019DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]","[:DateNoLine]","[:World]","6019"]



