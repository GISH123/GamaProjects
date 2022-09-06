from sql.mabi.bureport.BUReport_1000 import BUReport_1000
from sql.mabi.bureport.BUReport_1100 import BUReport_1100
from sql.mabi.bureport.BUReport_1800 import BUReport_1800
from sql.mabi.bureport.BUReport_6000 import BUReport_6000

class ReportMain(BUReport_1000,BUReport_1100,BUReport_1800,BUReport_6000) :
    def __init__(self):
        pass

