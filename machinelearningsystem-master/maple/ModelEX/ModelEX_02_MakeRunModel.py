import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling
import datetime
# 該程式主要為基本執行模型的檔案

if __name__ == "__main__":

    makeModeling.main({
        "runtype": ["runmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-06-17"]
        , "productname": ["maple"]
        , "project": ["ModelEX"]
        # , "rawdataversion": ["R1_0_1"]
        # , "preprocessversion": ["P1_0_1"]
        # , "usemodelversion": ["M1_0_1"]
        , "modelversion": ["V1_0_1"]
        # , "runstep": ["rawdata", "preprocess", "usemodel"]
    })


    # startDateTime = datetime.datetime.strptime( "2022-01-02", "%Y-%m-%d")
    # endDateTime = datetime.datetime.strptime( "2022-01-31", "%Y-%m-%d")
    # makeDatetime = startDateTime
    # while makeDatetime <= endDateTime:
    #     makeModeling.main({
    #         "runtype": ["runmodel"]                                         # 只能輸入 buildmodel 或 runmodel , 只會取第一個值
    #         , "builduser": ["vicying"]                                      # 只能輸入作者名 , 只會取第一個值
    #         , "makedate": [makeDatetime.strftime("%Y-%m-%d")]               # 只能輸入日期格式為YYYY-MM-DD , 取多個值
    #         , "productname": ["ExampleProduct"]                             # 只能輸入產品名 , 只會取第一個值
    #         , "project": ["CheckProject"]                             # 只能輸入計畫名 , 只會取第一個值
    #         , "modelversion": ["V0_0_56","V0_0_57","V0_0_58"]               # 只能輸入格式為V0_0_1 , 在 runtype 為 buildmodel 取單一值 , 在 runtype 為 runmodel 取多個值
    #     })
    #     makeDatetime = makeDatetime + datetime.timedelta(days=1)

