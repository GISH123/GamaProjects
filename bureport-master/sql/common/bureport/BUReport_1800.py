import time
from sql.common.bureport.BUReport_1100 import BUReport_1100 as BUReport_1100_Common

class BUReport_1800() :

    @classmethod
    def insert1804DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]","[:DateNoLine]","COMMON","1804"]


