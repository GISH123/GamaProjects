import sys # 模組，sys指向這個模組物件
import inspect
from info.maple.tableinfo.TableInfoMain import TableInfoMain as MapleTableInfoMain

class Animal():
    def __init__(self):
        self.legs = 2
        self.name = 'Dog'
        self.color= 'Spotted'
        self.smell= 'Alot'
        self.age  = 10
        self.kids = 0

    def getqlwe(self):
        pass


    def getqlddwe(self):
        pass


    def getqldsdwe(self):
        pass

tableInfoMain = MapleTableInfoMain()
timFunctionNameArr = dir(tableInfoMain)

initFunctionArr = ['__class__', '__delattr__', '__dict__', '__dir__', '__doc__'
        , '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__'
        , '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__'
        , '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__'
        , '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__'
        , '__weakref__', 'age', 'color', 'kids', 'legs', 'name', 'smell']

# initFunctionArr = ['__class__', '__delattr__', '__dict__', '__dir__', '__doc__'
#         , '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__'
#         , '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__'
#         , '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__'
#         , '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__'
#         , '__weakref__', 'age', 'color', 'kids', 'legs', 'name', 'smell']

useFunctionNameArr = []

for timFunctionName in timFunctionNameArr :
    if 'getBUReport' in timFunctionName \
        and 'Info' in timFunctionName \
        and 'Statistics' not in timFunctionName :
        useFunctionNameArr.append(timFunctionName)

    #print(timFunctionName)


makeInfo = {
    "tableauReportName" : ""
    , "gameName" :"wod"
    , "gameCHName" : ""
    , "schemaName" : ""
    , "serverName": ""
    , "serverPort": ""
    , "dbName": ""
    , "userName": ""
}

dataColumnArr = [
    "commondata_1", "commondata_2", "commondata_3", "commondata_4", "commondata_5"
    , "commondata_6", "commondata_7", "commondata_8", "commondata_9", "commondata_10"
    , "commondata_11", "commondata_12", "commondata_13", "commondata_14", "commondata_15"
    , "uniqueint_1", "uniqueint_2", "uniqueint_3", "uniqueint_4", "uniqueint_5"
    , "uniqueint_6", "uniqueint_7", "uniqueint_8", "uniqueint_9", "uniqueint_10"
    , "uniqueint_11", "uniqueint_12", "uniqueint_13", "uniqueint_14", "uniqueint_15"
    , "uniquestr_1", "uniquestr_2", "uniquestr_3", "uniquestr_4", "uniquestr_5"
    , "uniquestr_6", "uniquestr_7", "uniquestr_8", "uniquestr_9", "uniquestr_10"
    , "uniquestr_11", "uniquestr_12", "uniquestr_13", "uniquestr_14", "uniquestr_15"
    , "uniquestr_16", "uniquestr_17", "uniquestr_18", "uniquestr_19", "uniquestr_20"
    , "uniquedbl_1", "uniquedbl_2", "uniquedbl_3", "uniquedbl_4", "uniquedbl_5"
    , "uniquedbl_6", "uniquedbl_7", "uniquedbl_8", "uniquedbl_9", "uniquedbl_10"
    , "uniquedbl_11", "uniquedbl_12", "uniquedbl_13", "uniquedbl_14", "uniquedbl_15"
    , "uniquedbl_16", "uniquedbl_17", "uniquedbl_18", "uniquedbl_19", "uniquedbl_20"
    , "uniquetime_1", "uniquetime_2", "uniquetime_3"
    , "otherstr_1", "otherstr_2", "otherstr_3", "otherstr_4", "otherstr_5"
    , "otherstr_6", "otherstr_7", "otherstr_8", "otherstr_9", "otherstr_10"
    , "uniquearray_1", "uniquearray_2", "uniquejson_1"
]

for useFunctionName in  useFunctionNameArr :
    print(useFunctionName)
    columnInfoMap = eval(f"tableInfoMain.{useFunctionName}({makeInfo})[1]")
    columnSQL = ""
    for dataColumn in dataColumnArr:
        if dataColumn != columnInfoMap[dataColumn]["description"]:
            print("{} {} {}".format(useFunctionName, dataColumn , columnInfoMap[dataColumn]["description"]))







# {'kids': 0, 'name': 'Dog', 'color': 'Spotted', 'age': 10, 'legs': 2, 'smell': 'Alot'}
# now dump this in some way or another
#print(', '.join("%s: %s" % item for item in attrs.items()))