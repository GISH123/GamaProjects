from sql.lineageweb.bureportstatistics.BUReportStatistics_000 import BUReportStatistics_000
from sql.lineageweb.bureportstatistics.BUReportStatistics_100 import BUReportStatistics_100
from sql.lineageweb.bureportstatistics.BUReportStatistics_600 import BUReportStatistics_600
from sql.lineageweb.bureportstatistics.BUReportStatistics_Check import BUReportStatistics_Check


class ReportMain(BUReportStatistics_000, BUReportStatistics_100, BUReportStatistics_600, BUReportStatistics_Check):
    """
        類型1 順序型SQL ： "OrderInsert"
            "OrderInsert" , [OrderInsertSQLCode1,OrderInsertSQLCode2 ..... ]
    """
    def __init__(self):
        pass



