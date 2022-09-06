import os
import pandas
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl

class UseModel_ExternalDataManage() :

    @classmethod
    def MakeUseModel_ExternalDataManage_M1_0_1(self, makeInfo):
        return "MakeUseModelFreeFuction", None , {}
