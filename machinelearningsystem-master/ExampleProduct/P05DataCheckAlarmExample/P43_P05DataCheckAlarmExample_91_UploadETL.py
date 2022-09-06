import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.sshCtrl import SSHCtrl
sshCtrl = SSHCtrl("10.10.99.131", 22, "root", pkey="env/ALL_PKEY_ROOT")


# print("====================================================================================================")
# sshCtrl.ssh_exec_cmd("rm -rf /mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("mkdir /mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.sftp_put_dir("./env_linux","/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer/env")
# sshCtrl.sftp_put_dir("./package","/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer/package")
# sshCtrl.sftp_put_dir("./file","/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer/file")
# sshCtrl.sftp_put_dir("./project","/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer/project")
# sshCtrl.ssh_exec_cmd("cp -r env project/05_WebCrawler",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r file project/05_WebCrawler",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r package project/05_WebCrawler",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r env project/06_MakeData",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r file project/06_MakeData",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r package project/06_MakeData",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r env project/07_CheckData",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r file project/07_CheckData",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r package project/07_CheckData",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r env project/08_Crontab",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r file project/08_Crontab",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("cp -r package project/08_Crontab",path="/mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# sshCtrl.ssh_exec_cmd("chmod -R 777 /mfs/Docker/Python37/Volumes/Library/AppAnnieGamer")
# print("====================================================================================================")
#
#
