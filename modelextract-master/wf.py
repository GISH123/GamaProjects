import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv

load_dotenv(dotenv_path="env/hive.env")
# SQLCtrl
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='gtwpd'
    , auth_mechanism='PLAIN'
)

sql = """
SELECT * 
FROM wf_all.pinball_log_2nd_log_login
WHERE dt > '20211101' AND viewer_id = 890713872
LIMIT 100
"""



pd = hiveCtrl.searchSQL(sql)

print(pd)

