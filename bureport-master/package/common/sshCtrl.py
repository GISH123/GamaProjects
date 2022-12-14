import os
import paramiko
import stat
import traceback
from datetime import datetime
from functools import wraps
from dotenv import load_dotenv

class SSHCtrl:
    def __init__(self, host=None, port=None, usr=None, passwd=None, pkey=None, env=None, timeout=30, useSftp=False):
        if env == None:
            self._host = host
            self._port = port
            self._usr = usr
            self._passwd = passwd
            self._pkey = pkey
        else:
            load_dotenv(dotenv_path=env)
            self._env = env
            self._host = os.getenv("SSH_IP")
            self._port = int(os.getenv("SSH_PORT"))
            self._usr = os.getenv("SSH_USER")
            self._passwd = os.getenv("SSH_PASSWD")
            self._pkey = os.getenv("SSH_PKEY")
        self._timeout = timeout
        self._ssh = None
        self._sftp = None
        self._ssh_connect()
        if useSftp == True:
            self._sftp_connect()


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
            else:
                key = paramiko.RSAKey.from_private_key_file(self._pkey)
                transport.connect(username=self._usr, pkey=key)
            self._sftp = paramiko.SFTPClient.from_transport(transport)
        except Exception as e:
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

    # ????????????====================================================================================================

    # ??????ssh????????????????????????????????????????????????
    def reconnect(self, useSftp=False):
        self._ssh.close()
        self._ssh_connect()
        if useSftp == True:
            self._sftp.close()
            self._sftp_connect()


    #??????ssh????????????????????????????????????????????????
    def ssh_exec_cmd(self, cmd, path="~",timeout=None):
        try:
            result = self._exec_command("cd " + path + ";" + cmd,timeout=timeout)
            print(result)
        except Exception as e:
            raise RuntimeError("exec cmd [%s] failed" % cmd)

    # ?????????????????? ??????????????????
    def ssh_exec_cmd_return(self, cmd, path="~",timeout=None):
        try:
            result = self._exec_command_return("cd " + path + ";" + cmd,timeout=timeout)
            return result
        except Exception as e:
            raise RuntimeError("exec cmd [%s] failed" % cmd)

    # ??????ssh????????????????????????????????????????????????
    def ssh_exec_cmd_singleconnect(self, cmd, path="~", timeout=None):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if self._pkey == None:
                ssh.connect(hostname=self._host, port=self._port, username=self._usr, password=self._passwd, timeout=self._timeout)
            else:
                key = paramiko.RSAKey.from_private_key_file(self._pkey)
                ssh.connect(hostname=self._host, port=self._port, username=self._usr, timeout=self._timeout, pkey=key)

            try:
                stdin, stdout, stderr = ssh.exec_command("cd " + path + ";" + cmd, get_pty=True, timeout=timeout)
                for line in iter(stdout.readline, ""):
                    print(line, end="")
                return stdout.read().decode()
            except Exception as e:
                raise RuntimeError("Exec command [%s] failed" % str(cmd))
            print(result)
        except Exception as e:
            raise RuntimeError("exec cmd [%s] failed" % cmd)
        finally:
            ssh.close()

    # ???????????????sh???????????????
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

    # ??????sftp???????????????????????????
    def _upload_file(self, local_file, remote_file):
        try:
            self._sftp.put(local_file, remote_file)
        except Exception as e:
            raise RuntimeError("upload failed [%s]" % str(e))

    # ??????sftp???????????????????????????
    def _download_file(self, remote_file, local_file):
        try:
            self._sftp.get(remote_file, local_file)
        except Exception as e:
            raise RuntimeError("upload failed [%s]" % str(e))

    # ???????????????????????????????????????
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

    # ???????????????????????????????????????
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

    # ????????????====================================================================================================

    # ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
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

    # ??????ssh??????????????????
    def _exec_command(self, cmd, timeout=None):
        try:
            stdin, stdout, stderr = self._ssh.exec_command(cmd, get_pty=True, timeout=timeout)
            for line in iter(stdout.readline, ""):
                print(line, end="")
            return stdout.read().decode()
        except Exception as e:
            raise RuntimeError("Exec command [%s] failed" % str(cmd))

    # ??????ssh???????????????????????????
    def _exec_command_return(self, cmd, timeout=None):
        try:
            stdin, stdout, stderr = self._ssh.exec_command(cmd, get_pty=True, timeout=timeout)
            return stdout.read().decode()
        except Exception as e:
            raise RuntimeError("Exec command [%s] failed" % str(cmd))

    # ?????????????????????????????????
    def _get_all_files_in_remote_dir(self, sftp, remote_dir):
        all_files = list()
        #if remote_dir[-1] == '/':
        #    remote_dir = remote_dir[0:-1]
        files = sftp.listdir_attr(remote_dir)
        for file in files:
            filename = remote_dir + '/' + file.filename
            if stat.S_ISDIR(file.st_mode):  # ???????????????????????????
                all_files.extend(self._get_all_files_in_remote_dir(sftp, filename))
            else:
                all_files.append(filename)
        return all_files

        # ???????????????????????????????????????

    # ?????????????????????????????????
    def _get_all_files_in_local_dir(self, local_dir):
        all_files = list()
        for root, dirs, files in os.walk(local_dir, topdown=True):
            for file in files:
                filename = os.path.join(root, file)
                filename = filename.replace("\\","/")
                all_files.append(filename)
        return all_files

