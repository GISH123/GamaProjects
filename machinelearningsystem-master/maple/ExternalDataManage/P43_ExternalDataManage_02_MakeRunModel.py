import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling
import datetime
import time

# 該程式主要為基本執行模型的檔案

if __name__ == "__main__":
    st = time.time()
    makeModeling.main({
        "runtype": ["runmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-06-14"]
        , "productname": ["maple"]
        , "project": ["ExternalDataManage"]
        , "modelversion": ["V1_0_1"]  # 只能輸入格式為V0_0_1 , 在 runtype 為 buildmodel 取單一值 , 在 runtype 為 runmodel 取多個值
        , "parameter": {
            'ProjectPath': 'D:\\Git\\machinelearningsystem\\maple\\ExternalDataManage\\file\\ModelData',
            'ExternalDataPath': 'Common',
            'RawVersion': 'R1_0_1',
            'PreVersion': 'P1_0_1',
            'ModelVersion': 'M1_0_1',
            'ExternalDataFile': '2021_2022equipment_revised_ALL.csv'
        }
    })
    ed = time.time()
    print(st-ed)
