import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import datetime

import common.P43_01_MakeModelingCommon as makeModeling

# 此範例主要是告知如何製作相關資料檢查info
# 請查看
#   info\ModelResultInfo_P04DataCheckInfoExample.py
#   info\ModelScoreInfo_P04DataCheckInfoExample.py
#   info\RawDataInfo_P04DataCheckInfoExample.py
#   info\PreProcessInfo_P04DataCheckInfoExample.py
#   info\UseModelInfo_P04DataCheckInfoExample.py
#
#   會根據 "rawdataversion" , "preprocessversion" , "usemodelversion" , "modelversion" 裡列出的相關內容去做相關的資料檢查
#   請查看各自的 info\[:Step]Info_P04DataCheckInfoExample.py 以下範例就會跑相關檢查
#   start check ExampleProduct P04DataCheckInfoExample ModelResult , version is V0_0_3 , date is 2022-01-01
#   end check ExampleProduct P04DataCheckInfoExample ModelResult , version is V0_0_3 , date is 2022-01-01
#   start check ExampleProduct P04DataCheckInfoExample ModelScore , version is V0_0_3 , date is 2022-01-01
#   end check ExampleProduct P04DataCheckInfoExample ModelResult , version is V0_0_3 , date is 2022-01-01
#   start check ExampleProduct P04DataCheckInfoExample RawData , version is R1_0_3 , date is 2022-01-01
#   end check ExampleProduct P04DataCheckInfoExample RawData , version is R1_0_3 , date is 2022-01-01
#   start check ExampleProduct P04DataCheckInfoExample PreProcess , version is P1_0_3 , date is 2022-01-01
#   end check ExampleProduct P04DataCheckInfoExample PreProcess , version is P1_0_3 , date is 2022-01-01
#   start check ExampleProduct P04DataCheckInfoExample UseModel , version is M1_0_3 , date is 2022-01-01
#   end check ExampleProduct P04DataCheckInfoExample UseModel , version is M1_0_3 , date is 2022-01-01

if __name__ == "__main__":
    startDateTime = datetime.datetime.strptime("2022-01-01", "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime("2022-01-10", "%Y-%m-%d")
    makeDatetime = startDateTime
    while makeDatetime <= endDateTime:
        makeModeling.main({
            "runtype": ["checkdata"]
            , "makedate": [makeDatetime.strftime("%Y-%m-%d")]
            , "productname": ["ExampleProduct"]
            , "project": ["P04DataCheckInfoExample"]
            , "rawdataversion": ["R1_0_3"]
            , "preprocessversion": ["P1_0_3"]
            , "usemodelversion": ["M1_0_3"]
            , "modelversion": ["V0_0_3"]
        })
        makeDatetime = makeDatetime + datetime.timedelta(days=1)



