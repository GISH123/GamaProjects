from sql.mabi.bureportstatistics.BUReportStatistics_000 import BUReportStatistics_000
from sql.mabi.bureportstatistics.BUReportStatistics_100 import BUReportStatistics_100
from sql.mabi.bureportstatistics.BUReportStatistics_600 import BUReportStatistics_600
from sql.mabi.bureportstatistics.BUReportStatistics_Check import BUReportStatistics_Check

class ReportMain(BUReportStatistics_000,BUReportStatistics_100,BUReportStatistics_600,BUReportStatistics_Check) :
    def __init__(self):
        pass



