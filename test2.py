# coding=utf-8
import re, sys, datetime, os, demjson
from pymongo import MongoClient
from bson.son import SON


def init_mongo_db(db_name=['analysis']):
    mongo_client = MongoClient(host="120.92.133.11", port=27077, connect=False)
    return mongo_client[db_name[0]]


def init_game_db(db_name=['fire2']):
    mongo_client = MongoClient(host="123.59.71.187", port=27027, connect=False)
    return mongo_client[db_name[0]]


db = init_mongo_db()
game_db = init_game_db()


def daily_match_reward(days=1):
    collect_date = datetime.datetime.now() - datetime.timedelta(days=days)
    collect_date_string = datetime.datetime.strftime(collect_date, "%Y-%m-%d")
    collection_name = 'log_date.' + collect_date_string
    match_infos = list(db[collection_name].find({'type': 'match'}))
    toWhoms = short_match_info_list(match_infos, short_length=20)
    # for toWhom in toWhoms:
    reward_mail = {
        'template': 0,
        'content': {'0': 11},
        'params': ['daily match reward'],
        'createAt': datetime.datetime.utcnow(),
        'toWhichZone': [],
        'toWhom': toWhoms
    }
    game_db.systemMail.insert_one(reward_mail)


def short_match_info_list(match_infos, short_length=50):
    players = []
    for i in range(0, match_infos.__len__()):
        fake_match_count = 0
        # if match_infos[i]['playerId'] == '12101137348089':
        #     l = dict()
        for m in match_infos[i]['matches']:
            if m['expAdd'] <= 0:
                fake_match_count += 1
        if match_infos[i]['matches'].__len__() - fake_match_count > 4:
            players.append(long(match_infos[i]['playerId']))
    return players
    # if i % short_length == short_length - 1:
    #     yield players
    #     players = []
    # if match_infos.__len__() % short_length and i == match_infos.__len__() - 1:
    #     yield players


def daily_rank_reward(days=1):
    collect_date = datetime.datetime.now() - datetime.timedelta(days=days)
    collect_date_string = datetime.datetime.strftime(collect_date, "%Y-%m-%d")
    collection_name = 'log_date.' + collect_date_string
    match_infos = list(db[collection_name].find({'type': 'match'}))
    toWhom = []
    for m in match_infos:
        cnt = 0
        begin_time = datetime.datetime.strptime(collect_date_string, '%Y-%m-%d') + datetime.timedelta(hours=19)
        end_time = begin_time + datetime.timedelta(hours=3)
        for info in m['matches']:
            finished_time = datetime.datetime.strptime(info['finishedTime'], '%Y-%m-%d %H:%M:%S')
            if begin_time <= finished_time <= end_time and info['gameType'] == '1':
                cnt += 1
        if cnt > 2:
            toWhom.append(info['playerId'])
    reward_mail = {
        'template': 6,
        'content': {'170': 3},
        'params': [u'感谢您积极参加天梯比赛，更新之后，天梯比赛可以正常进行了，这是您昨天参加天梯时客户端闪退的补偿，请再接再厉继续征战。'],
        'createAt': datetime.datetime.utcnow(),
        'toWhichZone': [],
        'toWhom': toWhom
    }
    game_db.systemMail.insert_one(reward_mail)


def retention(days=2):
    start_date = datetime.datetime.now() - datetime.timedelta(days=days)
    start_date_string = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    start_collection_name = 'log_date.' + start_date_string
    start_login_collection = list(db[start_collection_name].find({'type': 'daily_login'}))
    retention_people = dict()
    for player in start_login_collection:
        retention_people[player['playerId']] = False
    for k in range(1, days):
        end_date = start_date + datetime.timedelta(days=k)
        end_date_string = datetime.datetime.strftime(end_date, "%Y-%m-%d")
        end_collection_name = 'log_date.' + end_date_string
        # end_login_collection = list(db[end_collection_name].find({'type': 'daily_login'}))
        # if end_login_collection:
        #     for info in end_login_collection:
        #         if retention_people.has_key(info['playerId']):
        #             retention_people[info['playerId']] = True
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
        'start_date': start_date_string,
        'end_date': end_date_string,
        'retention_type': days - 1,
        'retention_people': r
    })


def collect_paying_player(days=2):
    for i in range(1, days):
        collect_date = datetime.datetime.now() - datetime.timedelta(days=i)
        collect_date_string = datetime.datetime.strftime(collect_date, "%Y-%m-%d")
        collection_name = 'log_date.' + collect_date_string
        paying_players = list(db[collection_name].find({'type': 'payment'}))
        if paying_players:
            for info in paying_players:
                paying_player = db['paying_player'].find_one({'playerId': info['playerId']})
                if not paying_player:
                    db['paying_player'].insert_one({'playerId': info['playerId'],
                                                    'first_pay_date': collect_date_string})
                    # orders = []
                    # if paying_players:
                    #     orders = paying_player['orders']
                    #     orders.append(info)
                    #     db['paying_player'].update_one({'playerId': info['playerId']}, {
                    #         '$set': {'orders': orders}})
                    # else:
                    #     orders.append(info)
                    #     db['paying_player'].insert_one({
                    #         'playerId': info['playerId'],
                    #         'orders': orders})


print datetime.datetime.now()
daily_rank_reward()
print datetime.datetime.now()
