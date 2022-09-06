from sql.lineage.bureport.BUReport_1000 import BUReport_1000
from sql.lineage.bureport.BUReport_1100 import BUReport_1100
from sql.lineage.bureport.BUReport_1800 import BUReport_1800
from sql.lineage.bureport.BUReport_6000 import BUReport_6000
from sql.lineage.bureport.BUReport_20000 import BUReport_20000


class ReportMain(BUReport_1000, BUReport_1100, BUReport_1800, BUReport_6000,BUReport_20000):
    """
        類型1 順序型SQL ： "OrderInsert"
            "OrderInsert" , [OrderInsertSQLCode1,OrderInsertSQLCode2 ..... ]
        類型2 多重Insert : "MutiInsert"
            "MutiInsert" , [fromSQLCode,MutiInsertSQLCode]
        類型9 直接搬移 : "MoveModelExtract"
            "MoveModelExtract" , ["[:GameName]","[:DateNoLine]","[:World]","[:TableNumber]"]
    """
    def __init__(self):
        pass

