import os
import git
import shutil
new_repo = git.Repo.clone_from(url="http://10.10.99.191:8888/gtw_pd/common_pythonlib_package.git", to_path='.init/common_pythonlib_package')
os.mkdir("package/")
shutil.copytree(".init/common_pythonlib_package/package/common", "package/common")
