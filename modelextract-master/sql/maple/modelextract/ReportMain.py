from sql.maple.modelextract.ModelExtract_1000 import ModelExtract_1000
from sql.maple.modelextract.ModelExtract_1100 import ModelExtract_1100
from sql.maple.modelextract.ModelExtract_1800 import ModelExtract_1800
from sql.maple.modelextract.ModelExtract_2000 import ModelExtract_2000
from sql.maple.modelextract.ModelExtract_3000 import ModelExtract_3000
from sql.maple.modelextract.ModelExtract_4000 import ModelExtract_4000
from sql.maple.modelextract.ModelExtract_5000 import ModelExtract_5000
from sql.maple.modelextract.ModelExtract_6000 import ModelExtract_6000
from sql.maple.modelextract.ModelExtract_11000 import ModelExtract_11000
from sql.maple.modelextract.ModelExtract_13000 import ModelExtract_13000
from sql.maple.modelextract.ModelExtract_15000 import ModelExtract_15000
from sql.maple.modelextract.ModelExtract_16000 import ModelExtract_16000
from sql.maple.modelextract.ModelExtract_17000 import ModelExtract_17000

class ReportMain(
        ModelExtract_1000
        , ModelExtract_1100
        , ModelExtract_1800
        , ModelExtract_2000
        , ModelExtract_3000
        , ModelExtract_4000
        , ModelExtract_5000
        , ModelExtract_6000
        , ModelExtract_11000
        , ModelExtract_13000
        , ModelExtract_15000
        , ModelExtract_16000
        , ModelExtract_17000
    ) :
    """
        類型1 順序型SQL ： "OrderInsert"
            "OrderInsert" , [OrderInsertSQLCode1,OrderInsertSQLCode2 ..... ]
        類型2 多重Insert : "MutiInsert"
            "MutiInsert" , [fromSQLCode,MutiInsertSQLCode]
    """
    def __init__(self):
        pass

