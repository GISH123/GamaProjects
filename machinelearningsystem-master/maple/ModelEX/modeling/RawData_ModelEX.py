import os
import pandas as pd
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl

class RawData_ModelEX() :

    @classmethod
    def MakeRawData_ModelEX_R1_0_1(self, makeInfo):
        return "MakeRawDataFreeFuction", None
