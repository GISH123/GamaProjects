
class SqlTool:
    # 载入SQL檔案
    def loadfile(self, filePath, encoding="utf8"):
        self.__sqlfile = open(filePath, "r", encoding=encoding)
        return self

    # 將相關內容字串取代成相關字元
    def makeSQLStrs(self,sqlReplaceArr=[["[:noReplace]",""]]):
        self.__sqlStrs = "".join(self.__sqlfile.readlines())
        for sqlReplace in sqlReplaceArr:
            self.__sqlStrs = self.__sqlStrs.replace(sqlReplace[0], sqlReplace[1])
        #self.__sqlStrs = self.__sqlStrs.replace("\n", " ")
        return self

    # 將字串切割成字串陣列
    def makeSQLArr(self):
        self.__sqlStrArr = self.__sqlStrs.split(";")[:-1]
        return self

    # 取得SQL字串陣列
    def getSQLArr(self):
        return self.__sqlStrArr
