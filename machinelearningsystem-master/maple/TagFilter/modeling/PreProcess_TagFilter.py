import os
import pandas as pd
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
import csv
from tqdm import tqdm
from os.path import exists
import math
from collections import defaultdict
from scipy import stats
from collections import Counter
import matplotlib
import matplotlib.pyplot as plt
from scipy.stats import kde
import numpy as np
from package.common.common.RawPreModel import RawPreModel

class PreProcess_TagFilter() :
    ####################################################################################################################
    # Tag - RANK()
    @classmethod
    def MakePreProcess_TagFilter_P0_9001_1(self, makeInfo, **parm_):
        return "MakePreProcessFreeFuction", None , {}