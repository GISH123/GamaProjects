import os
import pandas as pd
import numpy as np
import datetime
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.common.RawPreModel import RawPreModel
import math

class RawData_TagFilter() :

    @classmethod
    def MakeRawData_TagFilter_R0_9001_1(self, makeInfo, **parm_):
        return "MakeRawDataFreeFuction", None , {}