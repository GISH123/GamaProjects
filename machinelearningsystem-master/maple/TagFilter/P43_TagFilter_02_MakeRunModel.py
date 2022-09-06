import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling
import datetime
import time

# 該程式主要為基本執行模型的檔案

if __name__ == "__main__":
    makedate = [
        '2021-01-13', '2021-01-27','2021-02-10', '2021-02-24', '2021-03-10',
        '2021-03-31', '2021-04-14', '2021-04-28', '2021-05-12', '2021-05-26', '2021-06-09', '2021-06-23', '2021-07-07',
        '2021-07-21', '2021-08-04', '2021-08-18', '2021-09-08', '2021-09-29', '2021-10-20', '2021-11-03',
        '2021-11-17', '2021-12-01', '2021-12-15', '2021-12-29', '2022-01-12', '2022-01-26', '2022-02-09',
        '2022-02-23', '2022-03-16', '2022-04-06', '2022-04-27', '2022-05-11', '2022-06-01', '2022-06-22',
        '2022-07-06', '2022-07-27', '2022-08-10', '2022-08-24'
    ]
    for dt_ in makedate:
        startDateTime = datetime.datetime.strptime(dt_, "%Y-%m-%d")
        makeDatetime = startDateTime

        makeModeling.main({
            "runtype": ["runmodel"]  # 只能輸入 buildmodel 或 runmodel , 只會取第一個值
            , "builduser": ["peiyuwu"]  # 只能輸入作者名 , 只會取第一個值
            , "makedate": [makeDatetime.strftime("%Y-%m-%d")]  # 只能輸入日期格式為YYYY-MM-DD , 取多個值
            , "productname": ["maple"]  # 只能輸入產品名 , 只會取第一個值
            , "project": ["TagFilter"]  # 只能輸入計畫名 , 只會取第一個值
            , "modelversion": ["V0_9001_1"]   # 只能輸入格式為V0_0_1 , 在 runtype 為 buildmodel 取單一值 , 在 runtype 為 runmodel 取多個值
            , "parameter": {}
        })