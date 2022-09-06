import pandas
import time
import asyncio

class AsyncioSQLTool:

    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._startTime = time.time()

    # 若SQL中有分號，需先取代為[:SEMICOLON]
    def runsql(self, SQLCtrl, taskSQLStrArr,printError=False):
        if taskSQLStrArr != []:
            tasks = []
            semaphore = asyncio.Semaphore(40)
            count = 0
            for sqlStr in taskSQLStrArr:
                count = count + 1
                task = asyncio.ensure_future(self.send_req(SQLCtrl, sqlStr, semaphore, count , printError))
                tasks.append(task)
            self._loop.run_until_complete(asyncio.wait(tasks))

    async def send_req(self, SQLCtrl, insertDataSQL, semaphore, count,printError):
        async with semaphore:
            endTime = time.time()
            if printError == True:
                print("Send a request at", endTime - self._startTime, "seconds. {}".format(str(count)))
            try:
                res = await self._loop.run_in_executor(None, self.executeSQL, SQLCtrl, insertDataSQL,printError)
            except:
                print("error {}".format(str(count)))
                if printError == True:
                    print(insertDataSQL)
            endTime = time.time()
            if printError == True :
                print("Receive a response at", endTime - self._startTime, "seconds. {}".format(str(count)))

    def executeSQL(self,SQLCtrl,sqlStrs,printError):
        sqlStrArr = sqlStrs.split(";")[:-1]
        for sqlStr in sqlStrArr:
            # 若SQL中有分號，需先取代為[:SEMICOLON]
            sqlStr = sqlStr.replace("[:SEMICOLON]",";")
            self.executeSQL_TCByCount(SQLCtrl, sqlStr, 3, printError)

    def executeSQL_TCByCount(self, SQLCtrl , sql, TCCount, printError):
        excount = 1
        while 1 <= excount and excount <= TCCount:
            try:
                time.sleep((excount - 1) * 10)
                return SQLCtrl.executeSQL(sql)
            except Exception as e:
                excount = excount + 1
                print("Error SQL excount: {} \n".format(str(excount)))
                print(e if printError == True else "")
                print(sql if printError == True else "")
        print('info: Fail to execute SQL! \n' + sql)



