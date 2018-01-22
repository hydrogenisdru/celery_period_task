# coding=utf-8
import re, sys, datetime, os, demjson
from pymongo import MongoClient


#
# user = session.query(PlayerInfo).filter(PlayerInfo.userDesc == 'gogogo').one()

def init_mongo_db(db_name=['analysis']):
    mongo_client = MongoClient(host="120.92.133.11", port=27077, connect=False)
    return mongo_client[db_name[0]]


def init_game_db(db_name=['fire2']):
    mongo_client = MongoClient(host="120.92.216.136", port=27027, connect=False)
    # mongo_client = MongoClient(host="123.59.71.187", port=27027, connect=False)
    return mongo_client[db_name[0]]


db = init_mongo_db()
game_db = init_game_db()


def get_matches(days=1):
    now_date_string = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
    matches_finished = set()
    ranking_matches_finished = set()
    # os.chdir("/data/server/fire2/log/")
    os.chdir("./")
    L = []
    # for files in os.walk("/data/server/fire2/log/"):
    for files in os.walk("./"):
        for file in files:
            L.append(file)

    for value in L[2]:
        if re.match(r'gameServer.*.log$', os.path.basename(value), flags=re.MULTILINE):
            print value
            with open(value, 'r') as f:
                for line in f.readlines():
                    if re.search(r'StatGameResult ', line):
                        info = format_match_info(line_txt=line)
                        if info['gameType'] == '0':
                            matches_finished.add(info['gameSessionId'])
                        if info['gameType'] == '1':
                            ranking_matches_finished(info['gameSessionId'])

    print 'matches_finished'
    for key in matches_finished:
        print key
    print 'ranking_matches_finished'
    for key in ranking_matches_finished:
        print key


def format_match_info(line_txt):
    info = dict()
    info['gameSessionId'] = re.search(r'gameSessionId:\d{5,}', line_txt).group().split(":")[1].strip()
    info['gameType'] = re.search(r'gameType:\d+', line_txt).group().split(":")[1].strip()
    return info


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
        if m['playerId'] == '12103221429001':
            hello = 'Hola'
        for info in m['matches']:
            finished_time = datetime.datetime.strptime(info['finishedTime'], '%Y-%m-%d %H:%M:%S')
            if begin_time <= finished_time <= end_time and info['gameType'] == '1':
                cnt += 1
        if cnt > 2:
            toWhom.append(info['playerId'])
    reward_mail = {
        'template': 6,
        'content': {'170': 3},
        'params': [u'尊敬的枪神，您昨天在活动期间已经完成了3场天梯比赛，这是您的奖励，请再接再厉继续征战。'],
        'createAt': datetime.datetime.utcnow(),
        'toWhichZone': [],
        'toWhom': toWhom
    }


def xmas_battle_reward():
    collect_dates = ["2017-12-23", "2017-12-24"]
    toWhom = []
    for collect_date in collect_dates:
        collect_name = 'log_date.' + collect_date
        toWhom += lucky_filter(list(db[collect_name].find({'type': 'match'})))
    setToWhom = set(toWhom)
    reward_mail = {
        'template': 6,
        'content': {'50003': 1},
        'params': [u'恭喜您在活动中达到了奖励条件，特此献上头像框“冬季恋歌”，祝您节日愉快~'],
        'createAt': datetime.datetime.utcnow(),
        'toWhichZone': [],
        'toWhom': list(setToWhom)
    }
    game_db.systemMail.insert_one(reward_mail)


def lucky_filter(match_infos):
    player = []
    for info in match_infos:
        cnt = 0
        for match in info['matches']:
            if match['result'] == 1:
                cnt += 1
        if cnt > 1:
            player.append(info['playerId'])
    return player


def xmas_skin_reward():
    toWhom = []
    for p in list(game_db['commonStatInfo'].find({'allMatchCount': {'$gte': 50}})):
        toWhom.append(p['playerId'])
    reward_mail = {
        'template': 6,
        'content': {'11000903': 1},
        'params': ['恭喜您在活动中达到了奖励条件，特此献上凯旋皮肤“圣诞糖果”，祝您游戏愉快~'],
        'createAt': datetime.datetime.utcnow(),
        'toWhichZone': [],
        'toWhom': toWhom
    }
    game_db.systemMail.insert_one(reward_mail)


