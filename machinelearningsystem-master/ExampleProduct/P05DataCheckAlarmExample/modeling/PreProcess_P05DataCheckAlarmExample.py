import os
import pandas
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl

class PreProcess_P05DataCheckAlarmExample() :

    @classmethod
    def MakePreProcess_P05DataCheckAlarmExample_P1_0_3(self, makeInfo):
        """
        MakePreProcessFileInsert 方法說明
            會撈取 return preprocessType , preprocessObject 的 preprocessObject 裡面放置DF資料
            就會將 preprocessObject 裡的DF上傳至HDFS上面，並且利用Hive建立與HDFS的Partition

            上傳路徑為 /user/GTW_PD/DB/Model/Usedata/product=[:ProductName]/project=[:Project]/version=[:Version]/dt=[:DateNoLine]/step=[:Step]
            像是該Function 就會上傳到/user/GTW_PD/DB/Model/Usedata/product=ExampleProduct/project=P05DataCheckAlarmExample/version=P1_0_3/dt=20211231/step=PreProcess
        """
        df = pandas.read_csv("ExampleProduct/P05DataCheckAlarmExample/file/data/ExampleProduct_P05DataCheckAlarmExample_R1_0_3_PreProcess_noallcolumn.csv", sep="\t")
        return "MakePreProcessFileInsertOverwrite", df , {"DataCount":21123}

