import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.sshCtrl import SSHCtrl
sshCtrl = SSHCtrl("10.10.99.131", 22, "root", pkey="env/ALL_PKEY_ROOT")


print(sshCtrl.ssh_exec_cmd_return("ls"))
