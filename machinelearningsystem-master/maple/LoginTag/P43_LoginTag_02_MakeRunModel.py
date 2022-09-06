import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling
import datetime

if __name__ == "__main__":
    # startDateTime = datetime.datetime.strptime( "2022-05-30", "%Y-%m-%d")
    # endDateTime = datetime.datetime.strptime( "2022-06-30", "%Y-%m-%d")
    # makeDatetime = startDateTime
    # while makeDatetime <= endDateTime:
    #
    #     makeModeling.main({
    #         "runtype": ["runmodel"]  # 只能輸入 buildmodel 或 runmodel , 只會取第一個值
    #         , "builduser": ["vicying"]  # 只能輸入作者名 , 只會取第一個值
    #         , "makedate": [makeDatetime.strftime("%Y-%m-%d")]  # 只能輸入日期格式為YYYY-MM-DD , 取多個值
    #         , "productname": ["maple"]  # 只能輸入產品名 , 只會取第一個值
    #         , "project": ["LoginTag"]  # 只能輸入計畫名 , 只會取第一個值
    #         , "modelversion": ["V0_0_7"]  # 只能輸入格式為V0_0_1 , 在 runtype 為 buildmodel 取單一值 , 在 runtype 為 runmodel 取多個值
    #     })
    #     makeDatetime = makeDatetime + datetime.timedelta(days=1)

    startDateTime = datetime.datetime.strptime( "2022-01-01", "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime( "2022-01-01", "%Y-%m-%d")
    makeDatetime = startDateTime
    while makeDatetime <= endDateTime:
        makeModeling.main({
            "runtype": ["runmodel"]  # 只能輸入 buildmodel 或 runmodel , 只會取第一個值
            , "builduser": ["vicying"]  # 只能輸入作者名 , 只會取第一個值
            , "makedate": [makeDatetime.strftime("%Y-%m-%d")]  # 只能輸入日期格式為YYYY-MM-DD , 取多個值
            , "productname": ["maple"]  # 只能輸入產品名 , 只會取第一個值
            , "project": ["LoginTag"]  # 只能輸入計畫名 , 只會取第一個值
            , "modelversion": ["V0_0_14"]  # 只能輸入格式為V0_0_1 , 在 runtype 為 buildmodel 取單一值 , 在 runtype 為 runmodel 取多個值
        })
        makeDatetime = makeDatetime + datetime.timedelta(days=1)
