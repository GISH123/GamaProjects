from sql.tdn.modelextract.ModelExtract_1000 import ModelExtract_1000
from sql.tdn.modelextract.ModelExtract_1100 import ModelExtract_1100
from sql.tdn.modelextract.ModelExtract_1800 import ModelExtract_1800
from sql.tdn.modelextract.ModelExtract_2000 import ModelExtract_2000
from sql.tdn.modelextract.ModelExtract_6000 import ModelExtract_6000
from sql.tdn.modelextract.ModelExtract_11000 import ModelExtract_11000

class ReportMain(
        ModelExtract_1000
        , ModelExtract_1100
        , ModelExtract_1800
        , ModelExtract_2000
        , ModelExtract_6000
        , ModelExtract_11000
    ) :
    """
        類型1 順序型SQL ： "OrderInsert"
            "OrderInsert" , [OrderInsertSQLCode1,OrderInsertSQLCode2 ..... ]
        類型2 多重Insert : "MutiInsert"
            "MutiInsert" , [fromSQLCode,MutiInsertSQLCode]
    """
    def __init__(self):
        pass

