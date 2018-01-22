from models import init_db, PlayerInfo
from pymongo import MongoClient
import datetime, sys


def init_mongo_db(db_name=['analysis']):
    mongo_client = MongoClient(host="120.92.133.11", port=27077, connect=False)
    return mongo_client[db_name[0]]


session = init_db()()
db = init_mongo_db()

mongo_set = set()
for p in list(db['log_date.2018-01-02'].find({'type': 'create_new_character'})):
    mongo_set.add(long(p['playerId']))

begin_date = datetime.datetime.strptime('2018-01-02', '%Y-%m-%d')
mysql_exclude_set = set()

for p in session.query(PlayerInfo).filter(PlayerInfo.createAt >= begin_date):
    if not mongo_set.__contains__(p.uuid):
        mysql_exclude_set.add(p.uuid)

session.close()

origin = sys.stdout
f = open('output.txt', 'w')
sys.stdout = f
for p in mysql_exclude_set:
    print p
sys.stdout = origin
f.close()
