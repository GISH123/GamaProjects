import os
import pandas
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl

# UseModel

class UseModel_P04DataCheckInfoExample() :

    @classmethod
    def MakeUseModel_P04DataCheckInfoExample_M1_0_3(self, makeInfo):
        """
        MakeUseModelFileInsert 方法說明
            會撈取 return useModelType , useModelObject 的 useModelObject 裡面放置DF資料
            就會將 useModelObject 裡的DF上傳至HDFS上面，並且利用Hive建立與HDFS的Partition

            上傳路徑為 /user/GTW_PD/DB/Model/Usedata/product=[:ProductName]/project=[:Project]/version=[:Version]/dt=[:DateNoLine]/step=[:Step]
            像是該Function 就會上傳到/user/GTW_PD/DB/Model/Usedata/product=ExampleProduct/project=P04DataCheckInfoExample/version=M1_0_3/dt=20211231/step=UseModel
        """
        df = pandas.read_csv("ExampleProduct/P04DataCheckInfoExample/file/data/ExampleProduct_P04DataCheckInfoExample_M1_0_3_UseModel_noallcolumn.csv", sep="\t")
        """
        # 模型建立............
        #
        #
        #
        #
        #
        """
        modelResultDF = df
        modelScoreDF = df
        return "MakeUseModelFileInsertOverwrite", [modelResultDF,modelScoreDF] , {"DataCount":21123}

