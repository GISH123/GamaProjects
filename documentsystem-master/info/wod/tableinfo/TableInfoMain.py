from info.wod.tableinfo.TableInfo_BUReportStatistics import TableInfo_BUReportStatistics
from info.wod.tableinfo.TableInfo_1000 import TableInfo_1000
from info.wod.tableinfo.TableInfo_1100 import TableInfo_1100
from info.wod.tableinfo.TableInfo_1800 import TableInfo_1800
from info.wod.tableinfo.TableInfo_6000  import TableInfo_6000
from info.wod.tableinfo.TableInfo_11000  import TableInfo_11000
from info.wod.tableinfo.TableInfo_16000  import TableInfo_16000

class TableInfoMain(
        TableInfo_BUReportStatistics
        , TableInfo_1000
        , TableInfo_1100
        , TableInfo_1800
        , TableInfo_6000
        , TableInfo_11000
        , TableInfo_16000
    ) :
    def __init__(self):
        pass

