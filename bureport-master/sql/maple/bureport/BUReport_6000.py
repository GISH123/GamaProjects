from sql.common.bureport.BUReport_6000 import BUReport_6000 as BUReport_6000_Common

class BUReport_6000(BUReport_6000_Common) :

    @classmethod
    def insert6509DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]", "[:DateNoLine]", "[:World]", "6509"]

    @classmethod
    def insert6609DataSQL(self, makeInfo):
        return "MoveModelExtract", ["[:GameName]", "[:DateNoLine]", "[:World]", "6609"]