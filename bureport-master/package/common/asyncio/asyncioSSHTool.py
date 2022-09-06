import pandas
import time
import asyncio
import re

class AsyncioSSHTool:

    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._startTime = time.time()

    # 若SQL中有分號，需先取代為[:SEMICOLON]
    def runSSH(self, sshCtrl, taskSSHStrsArr,printError=False):
        if taskSSHStrsArr != []:
            tasks = []
            semaphore = asyncio.Semaphore(40)
            count = 0
            for taskSSHStrs in taskSSHStrsArr:
                count = count + 1
                task = asyncio.ensure_future(self.sendReq(sshCtrl, taskSSHStrs, semaphore, count , printError))
                tasks.append(task)
            self._loop.run_until_complete(asyncio.wait(tasks))

    async def sendReq(self, sshCtrl, taskSSHStrs, semaphore, count,printError):
        async with semaphore:
            endTime = time.time()
            if printError == True:
                print("SSH Send a request at", endTime - self._startTime, "seconds. {}".format(str(count)))
            time.sleep(count%40/10)
            try:
                res = await self._loop.run_in_executor(None, self.sshExecCmd, sshCtrl, taskSSHStrs,printError)
            except Exception as e:
                print("SSH error {}".format(str(count)))
                print(e if printError == True else "")
                print(taskSSHStrs if printError == True else "")
            endTime = time.time()
            if printError == True :
                print("SSH Receive a response at", endTime - self._startTime, "seconds. {}".format(str(count)))

    def sshExecCmd(self,sshCtrl,taskSSHStrs,printError):
        sshStrArr = taskSSHStrs.split("\n")
        for sshStr in sshStrArr:
            self.sshExecCmd_TCByCount(sshCtrl, sshStr, 3, printError)

    def sshExecCmd_TCByCount(self, sshCtrl, sshStr, TCCount, printError):
        excount = 1
        while 1 <= excount and excount <= TCCount:
            try:
                time.sleep((excount - 1) * 10)
                if re.search('[^a-zA-Z0-9-_ /.]+', sshStr) == None:
                    return sshCtrl.ssh_exec_cmd_singleconnect(sshStr)
            except Exception as e:
                excount = excount + 1
                print("SSH Error SSH excount: {} \n".format(str(excount)))
                print(e if printError == True else "")
                print(sshStr if printError == True else "")
        print('info: Fail to execute SSH! \n' + sshStr)



