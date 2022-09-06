from sql.wf.modelextract.ModelExtract_1100 import ModelExtract_1100
from sql.wf.modelextract.ModelExtract_6000 import ModelExtract_6000

class ReportMain(ModelExtract_1100, ModelExtract_6000) :
    """
        類型1 順序型SQL ： "OrderInsert"
            "OrderInsert" , [OrderInsertSQLCode1,OrderInsertSQLCode2 ..... ]
        類型2 多重Insert : "MutiInsert"
            "MutiInsert" , [fromSQLCode,MutiInsertSQLCode]
    """
    def __init__(self):
        pass

