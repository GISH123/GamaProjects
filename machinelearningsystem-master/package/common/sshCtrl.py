import os
import paramiko
import stat
import traceback
from datetime import datetime
from functools import wraps
from dotenv import load_dotenv

class SSHCtrl:
    def __init__(self, host=None , port=None, usr=None, passwd=None,pkey=None,env=None,timeout=30):
        if env == None :
            self._host = host
            self._port = port
            self._usr = usr
            self._passwd = passwd
            self._pkey = pkey
        else :
            load_dotenv(dotenv_path=env)
            self._host = os.getenv("SSH_IP")
            self._port = int(os.getenv("SSH_PORT"))
            self._usr = os.getenv("SSH_USER")
            self._passwd = os.getenv("SSH_PASSWD")
            self._pkey = os.getenv("SSH_PKEY")
        self._timeout = timeout
        self._ssh = None
        self._sftp = None
        self._sftp_connect()
        self._ssh_connect()

    def __del__(self):
        if self._ssh:
            self._ssh.close()
        if self._sftp:
            self._sftp.close()

    def _sftp_connect(self):
        try:
            transport = paramiko.Transport((self._host, self._port))
            if self._pkey == None :
                transport.connect(username=self._usr, password=self._passwd)
            else :
                key = paramiko.RSAKey.from_private_key_file(self._pkey)
                transport.connect(username=self._usr, pkey=key)
            self._sftp = paramiko.SFTPClient.from_transport(transport)
        except Exception:
            raise RuntimeError("sftp connect failed [%s]" % str(e))

    def _ssh_connect(self):
        try:
            self._ssh = paramiko.SSHClient()
            self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if self._pkey == None :
                self._ssh.connect(hostname=self._host,
                                  port=self._port,
                                  username=self._usr,
                                  password=self._passwd,
                                  timeout=self._timeout)
            else :
                key = paramiko.RSAKey.from_private_key_file(self._pkey)
                self._ssh.connect(hostname=self._host,
                                  port=self._port,
                                  username=self._usr,
                                  timeout=self._timeout,
                                  pkey=key)
        except Exception:
            raise RuntimeError("sftp connect failed [%s]" % str(e))

    @staticmethod
    def is_shell_file(file_name):
        return file_name.endswith(".sh")

    @staticmethod
    def is_file_exist(file_name):
        try:
            with open(file_name, "r"):
                return True
        except Exception as e:
            return False

    # 主要方法====================================================================================================

    #通過ssh連線到遠端伺服器，執行給定的命令
    def ssh_exec_cmd(self, cmd, path="~"):
        try:
            result = self._exec_command("cd " + path + ";" + cmd)
            print(result)
        except Exception:
            raise RuntimeError("exec cmd [%s] failed" % cmd)

    # 通過ssh連線到遠端伺服器，執行給定的命令並且回傳訊息
    def ssh_exec_cmd_return(self, cmd, path="~"):
        try:
            result = self._exec_command_return("cd " + path + ";" + cmd)
            return result
        except Exception:
            raise RuntimeError("exec cmd [%s] failed" % cmd)

    # 執行遠端的sh指令碼檔案
    def ssh_exec_shell(self, local_file, remote_file, exec_path):
        try:
            if not self.is_file_exist(local_file):
                raise RuntimeError("File [%s] not exist" % local_file)
            if not self.is_shell_file(local_file):
                raise RuntimeError("File [%s] is not a shell file" % local_file)

            self._check_remote_file(local_file, remote_file)

            result = self._exec_command("chmod +x " + remote_file + "; cd" + exec_path + ";/bin/bash " + remote_file)
            print("exec shell result: ", result)
        except Exception as e:
            raise RuntimeError("ssh exec shell failed [%s]" % str(e))

    # 通過sftp上傳本地檔案到遠端
    def _upload_file(self, local_file, remote_file):
        try:
            self._sftp.put(local_file, remote_file)
        except Exception as e:
            raise RuntimeError("upload failed [%s]" % str(e))

    # 通過sftp上傳本地檔案到遠端
    def _download_file(self, remote_file, local_file):
        try:
            self._sftp.get(remote_file, local_file)
        except Exception as e:
            raise RuntimeError("upload failed [%s]" % str(e))

    # 本地文件夾上傳到遠端伺服器
    def sftp_put_dir(self, local_dir, remote_dir):
        try:
            # if remote_dir[-1] == "/":
            #    remote_dir = remote_dir[0:-1]
            all_files = self._get_all_files_in_local_dir(local_dir)
            for file in all_files:
                remote_filename = file.replace(local_dir, remote_dir)
                remote_path = os.path.dirname(remote_filename)
                try:
                    self._sftp.stat(remote_path)
                except:
                    self._exec_command('mkdir -p %s' % remote_path)
                self._sftp.put(file, remote_filename)
                print("Loacl " + str(file) + " file to remote " + str(remote_filename) + " file.")
        except:
            print('ssh get dir from master failed.')
            print(traceback.format_exc())

    # 遠端伺服器下載到本地文件夾
    def sftp_get_dir(self, remote_dir, local_dir):
        try:
            all_files = self._get_all_files_in_remote_dir(self._sftp, remote_dir)
            for file in all_files:
                local_filename = file.replace(remote_dir, local_dir)
                local_filepath = os.path.dirname(local_filename)
                if not os.path.exists(local_filepath):
                    os.makedirs(local_filepath)
                self._sftp.get(file, local_filename)

        except:
            print('ssh get dir from master failed.')
            print(traceback.format_exc())  #

    # 輔助方法====================================================================================================

    # 檢測遠端的指令碼檔案和當前的指令碼檔案是否一致，如果不一致，則上傳本地指令碼檔案
    def _check_remote_file(self, local_file, remote_file):
        try:
            result = self._exec_command("find" + remote_file)
            if len(result) == 0:
                self._upload_file(local_file, remote_file)
            else:
                lf_size = os.path.getsize(local_file)
                result = self._exec_command("du -b" + remote_file)
                rf_size = int(result.split("\t")[0])
                if lf_size != rf_size:
                    self._upload_file(local_file, remote_file)
        except Exception as e:
            raise RuntimeError("check error [%s]" % str(e))

    # 通過ssh執行遠端命令
    def _exec_command(self, cmd):
        try:
            stdin, stdout, stderr = self._ssh.exec_command(cmd, get_pty=True)
            for line in iter(stdout.readline, ""):
                print(line, end="")
            return stdout.read().decode()
        except Exception as e:
            raise RuntimeError("Exec command [%s] failed" % str(cmd))

    # 通過ssh執行遠端命令並回傳
    def _exec_command_return(self, cmd):
        try:
            stdin, stdout, stderr = self._ssh.exec_command(cmd, get_pty=True)
            return stdout.read().decode()
        except Exception as e:
            raise RuntimeError("Exec command [%s] failed" % str(cmd))

    # 遞迴遠端所有目錄與文件
    def _get_all_files_in_remote_dir(self, sftp, remote_dir):
        all_files = list()
        #if remote_dir[-1] == '/':
        #    remote_dir = remote_dir[0:-1]
        files = sftp.listdir_attr(remote_dir)
        for file in files:
            filename = remote_dir + '/' + file.filename
            if stat.S_ISDIR(file.st_mode):  # 如果是文件遞迴處理
                all_files.extend(self._get_all_files_in_remote_dir(sftp, filename))
            else:
                all_files.append(filename)
        return all_files

        # 本地文件夾上傳到遠端伺服器

    # 遞迴本地所有目錄與文件
    def _get_all_files_in_local_dir(self, local_dir):
        all_files = list()
        for root, dirs, files in os.walk(local_dir, topdown=True):
            for file in files:
                filename = os.path.join(root, file)
                filename = filename.replace("\\","/")
                all_files.append(filename)
        return all_files

