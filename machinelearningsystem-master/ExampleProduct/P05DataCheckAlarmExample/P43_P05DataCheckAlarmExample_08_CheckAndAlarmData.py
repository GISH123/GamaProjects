import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import datetime

# 相關Info產生說明可以參考 P04DataCheckInfoExample
# 相關alarm產生說明請參考 P05DataCheckAlarmExample/info/ModelResultInfo_P05DataCheckAlarmExample

#  以下範例會產生相關輸出
#  start alarm ExampleProduct P04DataCheckInfoExample ModelResult , version is V0_0_3 , date is 2022-01-01
#  start alarm ExampleProduct P04DataCheckInfoExample ModelScore , version is V0_0_3 , date is 2022-01-01
#  start alarm ExampleProduct P04DataCheckInfoExample RawData , version is R1_0_3 , date is 2022-01-01
#  start alarm ExampleProduct P04DataCheckInfoExample PreProcess , version is P1_0_3 , date is 2022-01-01
#  start alarm ExampleProduct P04DataCheckInfoExample UseModel , version is M1_0_3 , date is 2022-01-01

import common.P43_01_MakeModelingCommon as makeModeling

if __name__ == "__main__":
    startDateTime = datetime.datetime.strptime( "2022-05-21", "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime( "2022-06-05", "%Y-%m-%d")
    makeDatetime = startDateTime
    while makeDatetime <= endDateTime:
        makeModeling.main({
            "runtype": ["checkdata"]
            , "makedate": [makeDatetime.strftime("%Y-%m-%d")]
            , "productname": ["ExampleProduct"]
            , "project": ["P05DataCheckAlarmExample"]
            , "rawdataversion": ["R1_0_3"]                  # 只產生相關 Info
            , "preprocessversion": ["P1_0_3"]               # 只產生相關 Info
            , "usemodelversion": ["M1_0_3"]                 # 只產生相關 Info
            , "modelversion": ["V0_0_4"]                    # 產生相關 Info 與 Alarm
        })
        makeDatetime = makeDatetime + datetime.timedelta(days=1)



