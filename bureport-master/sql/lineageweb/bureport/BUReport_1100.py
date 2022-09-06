import time
from sql.common.bureport.BUReportMuti_1100 import BUReportMuti_1100 as BUReportMuti_1100_Common


class BUReport_1100(BUReportMuti_1100_Common):

    @classmethod
    def insert1101DataSQL(self, makeInfo):
        return "MoveModelExtractMuti", ["[:GameName]", "[:DateNoLine]", "COMMON", "1101"]
