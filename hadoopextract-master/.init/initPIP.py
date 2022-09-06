import os
import git
import shutil
new_repo = git.Repo.clone_from(url="http://10.10.99.191:8888/gtw/pythonlibrary.git", to_path='.init/pythonlibrary')
shutil.copytree(".init/pythonlibrary/windows", "file")