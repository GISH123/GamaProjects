from sql.maple.bureport.BUReport_1000 import BUReport_1000
from sql.maple.bureport.BUReport_1100 import BUReport_1100
from sql.maple.bureport.BUReport_1800 import BUReport_1800
from sql.maple.bureport.BUReport_2000 import BUReport_2000
from sql.maple.bureport.BUReport_3000 import BUReport_3000
from sql.maple.bureport.BUReport_4000 import BUReport_4000
from sql.maple.bureport.BUReport_5000 import BUReport_5000
from sql.maple.bureport.BUReport_6000 import BUReport_6000
from sql.maple.bureport.BUReport_20000 import BUReport_20000
class ReportMain(
     BUReport_1000
    , BUReport_1100
    , BUReport_1800
    , BUReport_2000
    , BUReport_3000
    , BUReport_4000
    , BUReport_5000
    , BUReport_6000
    , BUReport_20000
) :
    def __init__(self):
        pass

