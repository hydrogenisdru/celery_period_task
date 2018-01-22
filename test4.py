# coding=utf-8
import re, sys, datetime, os, demjson
from pymongo import MongoClient
from models import PlayerInfo, init_db

session = init_db()()


#
# user = session.query(PlayerInfo).filter(PlayerInfo.userDesc == 'gogogo').one()

def init_mongo_db(db_name=['analysis']):
    mongo_client = MongoClient(host="120.92.133.11", port=27077, connect=False)
    return mongo_client[db_name[0]]


def init_game_db(db_name=['fire2']):
    mongo_client = MongoClient(host="120.92.216.136", port=27027, connect=False)
    return mongo_client[db_name[0]]


db = init_mongo_db()
game_db = init_game_db()


def get_realtime_data():
    now = datetime.datetime.now()
    now_date_string = datetime.datetime.strftime(now, "%Y-%m-%d")
    today_begin = datetime.datetime.strptime(now_date_string, '%Y-%m-%d')
    today_create, today_active = query_players_today()
    today_income_from_new_player, today_income_from_old_player, today_new_pay, today_old_pay \
        = query_today_income(today_now=now, today_begin=today_begin)
    result = {'saveTime': now, 'today_begin': today_begin, 'axisTime': datetime.datetime.strftime(now, "%H:%M:%S"),
              'today_create': today_create, 'today_active': today_active,
              'today_income_from_old_player': today_income_from_old_player,
              'today_income_from_new_player': today_income_from_new_player, 'today_old_pay': today_old_pay,
              'today_new_pay': today_old_pay}
    json_result = demjson.encode(result, encoding='utf-8')
    db.history_data.insert_one(result)
    return json_result


def query_players_today():
    os.chdir("/data/server/fire2/log/")
    L = []
    today_create = 0
    today_login = 0
    for files in os.walk("/data/server/fire2/log/"):
        for file in files:
            L.append(file)
    for value in L[2]:
        if re.match(r'gameServer*.log', os.path.basename(value)):
            with open(value, 'r') as f:
                for line in f.readlines():
                    if re.search(r'create player success', line):
                        today_create += 1
                    if re.search(r'login success', line):
                        today_login += 1
                    if re.search(r'loginBySessionToken success', line):
                        today_login += 1
    return today_create, today_login + today_create


# now = datetime.datetime.now()
# now_date_string = datetime.datetime.strftime(now, "%Y-%m-%d")
# today_begin = datetime.datetime.strptime(now_date_string, '%Y-%m-%d')
# ten_minutes = now - datetime.timedelta(minutes=10)
# c = query_new_player_count(now=now, begin=ten_minutes)
# tc = query_new_player_count(now=now, begin=today_begin)
# a = query_active_player_count(now=now, begin=ten_minutes)
# ta = query_active_player_count(now=now, begin=today_begin)
# ti, tp = query_today_income(today_now=now, today_begin=today_begin)
# result = {'saveTime': now, 'begin': ten_minutes, 'today_begin': today_begin,
#           'axisTime': datetime.datetime.strftime(now, "%H:%M:%S"), 'ten_minutes_create': c,
#           'today_create': tc,
#           'ten_minutes_active': a, 'today_active': ta, 'today_new_pay': tp,
#           'today_income': ti}
# json_result = demjson.encode(result, encoding='utf-8')
# db.history_data.insert_one(result)
# return json_result


def query_new_player_count(now, begin):
    p = session.query(PlayerInfo).filter(PlayerInfo.createAt <= now).filter(PlayerInfo.createAt >= begin).count()
    return p


def query_active_player_count(now, begin):
    p = session.query(PlayerInfo).filter(PlayerInfo.updateAt <= now).filter(PlayerInfo.updateAt >= begin).count()
    return p


def query_today_income(today_now, today_begin):
    income_from_old_player = 0
    income_from_new_player = 0
    today_pay = set()
    today_old_pay = set()
    # for p in game_db.order_platform.find({'date': {'$gte': today_begin, '$lte': today_now}}):
    for p in game_db.order_platform.find({'date': {'$gt': today_begin}}):
        cp_order_id = p['detail']['cp_order_id']
        uuid = cp_order_id.split('_')[1]
        if db.paying_player.find_one({'playerId': uuid}):
            income_from_old_player += income_filter(cp_order_id=cp_order_id)
            # if uuid not in today_old_pay:
            today_old_pay.add(uuid)
        else:
            income_from_new_player += income_filter(cp_order_id=cp_order_id)
            # if uuid not in today_pay:
            today_pay.add(uuid)
    return income_from_new_player, income_from_old_player, today_pay, today_old_pay


def income_filter(cp_order_id):
    k = cp_order_id.split('_')[2].split('.')
    credit = k[k.__len__() - 1]
    return int(re.search(r'\d+', credit).group().strip())


# print get_realtime_data()
def retention_s(begin_date, days=1):
    start_date = datetime.datetime.strptime(begin_date, '%Y-%m-%d')
    # start_date_string = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    start_collection_name = 'log_date.' + begin_date
    start_create_collection = list(db[start_collection_name].find({'type': 'create_new_character'}))
    retention_people = dict()
    for player in start_create_collection:
        retention_people[player['playerId']] = False
    # for k in range(1, days):
    end_date = start_date + datetime.timedelta(days=days)
    end_date_string = datetime.datetime.strftime(end_date, "%Y-%m-%d")
    end_collection_name = 'log_date.' + end_date_string
    for key in retention_people:
        if retention_people[key] == False and db[end_collection_name].find_one(
                {'type': 'daily_login', 'playerId': key}):
            retention_people[key] = True
    r = []
    for key in retention_people:
        if retention_people[key]:
            r.append(key)
    db['retention'].insert_one({
        'createAt': datetime.datetime.now(),
        'start_date': begin_date,
        'end_date': end_date_string,
        'retention_type': days,
        'retention_people': r
    })


def print_new_create(begin_date):
    start_date = datetime.datetime.strptime(begin_date, '%Y-%m-%d')
    # start_date_string = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    start_collection_name = 'log_date.' + begin_date
    start_create_collection = list(db[start_collection_name].find({'type': 'create_new_character'}))
    LogName = "new_create_" + begin_date + ".log"
    origin = sys.stdout
    f = open(LogName, 'w')
    sys.stdout = f
    print file
    for p in start_create_collection:
        print p['playerId']
    sys.stdout = origin
    f.close()


def print_daily_login(begin_date):
    start_date = datetime.datetime.strptime(begin_date, '%Y-%m-%d')
    # start_date_string = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    start_collection_name = 'log_date.' + begin_date
    start_create_collection = list(db[start_collection_name].find({'type': 'daily_login'}))
    LogName = "daily_login_" + begin_date + ".log"
    origin = sys.stdout
    f = open(LogName, 'w')
    sys.stdout = f
    print file
    for p in start_create_collection:
        print p['playerId']
    sys.stdout = origin
    f.close()


def gmt_change(beijing_time):
    if not isinstance(beijing_time, datetime.datetime):
        return None
    else:
        return beijing_time - datetime.timedelta(hours=8)


def compare_collection(date1, date2):
    # start_date_string = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    date1_collection_name = 'log_date.' + date1
    date1_collection = list(db[date1_collection_name].find({'type': 'create_new_character'}))
    date2_collection_name = 'log_date.' + date2
    date2_collection = list(db[date2_collection_name].find({'type': 'create_new_character'}))
    collection_dict = dict()
    for p in date1_collection:
        collection_dict[p['playerId']] = 1
    for p in date2_collection:
        if collection_dict.has_key(p['playerId']):
            collection_dict[p['playerId']] += 1
        else:
            collection_dict[p['playerId']] = 1
    print date1_collection.__len__(), date2_collection.__len__()
    print date1_collection.__len__() + date2_collection.__len__()
    print collection_dict.__len__()
    #
    # LogName = "collection_compare" + ".log"
    # origin = sys.stdout
    # f = open(LogName, 'w')
    # sys.stdout = f
    # print date1_collection.__len__() + date2_collection.__len__()
    # for key in collection_dict:
    #     print key
    # sys.stdout = origin
    # f.close()


ni, oi, np, op = query_today_income(today_now=gmt_change(datetime.datetime.now()),
                                    today_begin=gmt_change(datetime.datetime.strptime('2017-12-12', '%Y-%m-%d')))
print ni, oi
print np
print np.__len__()
print op
print op.__len__()
