# coding=utf-8
import re, sys, datetime, os, demjson
from pymongo import MongoClient


def init_mongo_db(db_name=['analysis']):
    mongo_client = MongoClient(host="120.92.133.11", port=27077, connect=False)
    return mongo_client[db_name[0]]


def init_game_db(db_name=['fire2']):
    mongo_client = MongoClient(host="120.92.216.136", port=27027, connect=False)
    # mongo_client = MongoClient(host="123.59.71.187", port=27027, connect=False)
    return mongo_client[db_name[0]]


db = init_mongo_db()
game_db = init_game_db()


def short_match_info_list(lst, short_length=50):
    players = []
    for i in range(0, lst.__len__()):
        players.append(lst[i])
        if i % short_length == short_length - 1:
            yield players
            players = []
        if lst.__len__() % short_length and i == lst.__len__() - 1:
            yield players
            players = []


# l = []
# for player in list(
#         game_db['player'].find({'registerDate': {'$lt': datetime.datetime.strptime('2017-12-23', '%Y-%m-%d')}})):
#     l.append(player['playerId'])
# print l.__len__()
# i = 0
# for toWhom in short_match_info_list(lst=l, short_length=200):
#     print 'line ' + str(i)
#     print toWhom
#     i += 1
#     reward_mail = {
#         'template': 6,
#         'content': {'1': 800},
#         'params': ['我们在最近的版本中将“安薇”调整为初始英雄，在此版本前创建的账号特此奉上800钻的补偿，祝您游戏愉快~~'],
#         'createAt': datetime.datetime.utcnow(),
#         'toWhichZone': [],
#         'toWhom': toWhom
#     }
#     game_db.systemMail.insert_one(reward_mail)
