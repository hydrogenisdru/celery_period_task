# coding=utf-8
import re, sys, datetime, os, demjson
from pymongo import MongoClient
from bson.son import SON


def init_mongo_db(url='mongodb://localhost:27027/', db_name=['analysis', 'fire2']):
    mongo_client = MongoClient(url)
    return mongo_client[db_name[0]], mongo_client[db_name[1]]


db, game_db = init_mongo_db()


def output_info(line_txt, type, pattern='currency'):
    info = dict()
    info['type'] = type
    info['datetime'] = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line_txt).group().strip()
    info['playerId'] = re.search(r'\d{5,}', line_txt).group().strip()
    if pattern == 'currency':
        info['id'] = re.search(r'currency: \d+', line_txt).group().split(" ")[1].strip()
    if pattern == 'item':
        info['id'] = re.search(r'itemId: \d+', line_txt).group().split(" ")[1].strip()
    info['count'] = re.search(r'count: \d+', line_txt).group().split(" ")[1].strip()
    info['reason'] = re.search(r'reason: \d+', line_txt).group().split(" ")[1].strip()
    return info


def payment_info(line_txt, type):
    info = dict()
    info['type'] = type
    info['datetime'] = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line_txt).group().strip()
    info['playerId'] = re.search(r'\d{5,}', line_txt).group().strip()
    info['trade_no'] = re.search(r'trade_no: .*goodsId', line_txt).group().split(" ")[1].strip()
    info['goodsId'] = re.search(r'goodsId: .*rmb', line_txt).group().split(" ")[1].strip()
    info['rmb'] = re.search(r'rmb: \d+', line_txt).group().split(" ")[1].strip()
    return info


def login_player_info(line_txt, type):
    info = dict()
    info['type'] = type
    info['login_datetime'] = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line_txt).group().strip()
    info['playerId'] = re.search(r'\d{5,}', line_txt).group().strip()
    return info


def get_match_info(line_txt, type):
    info = dict()
    info['type'] = type
    info['playerId'] = re.search(r'playerId:\d{5,}', line_txt).group().split(":")[1].strip()
    info['gameSessionId'] = re.search(r'gameSessionId:\d{5,}', line_txt).group().split(":")[1].strip()
    info['gameType'] = re.search(r'gameType:\d+', line_txt).group().split(":")[1].strip()
    is_win = re.search(r'win:\w+', line_txt).group().split(":")[1].strip() == 'true'
    is_draw = re.search(r'draw:\w+', line_txt).group().split(":")[1].strip() == 'true'
    if not is_win:
        if not is_draw:
            info['result'] = -1
        else:
            info['result'] = 0
    else:
        info['result'] = 1
    info['winningStreak'] = re.search(r'winningStreak:\d+', line_txt).group().split(":")[1].strip()
    info['losingStreak'] = re.search(r'losingStreak:\d+', line_txt).group().split(":")[1].strip()
    info['finishedTime'] = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line_txt).group().strip()
    return info


def create_player_info(line_txt, type):
    info = dict()
    info['type'] = type
    info['create_datetime'] = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line_txt).group().strip()
    info['playerId'] = re.search(r'\d{5,}', line_txt).group().strip()
    info['nick'] = re.search(r'nick:.*', line_txt).group().split(" ")[1].strip()
    return info


# now = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
now = datetime.datetime.now()
now_date_string = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
yesterday = now - datetime.timedelta(days=1)
yesterday_date_string = datetime.datetime.strftime(yesterday, "%Y-%m-%d")
analysis_content = dict()
analysis_content['analysis_create_new_character'] = []
analysis_content['analysis_daily_login'] = dict()
analysis_content['analysis_produce'] = []
analysis_content['analysis_consumption'] = []
analysis_content['analysis_payment'] = []
analysis_content['analysis_daily_matches'] = dict()
os.chdir("./")
L = []
for files in os.walk("./"):
    for file in files:
        L.append(file)

for value in L[2]:
    if re.match(r'GameServer.*.log(.*\d{4}\-\d{2}\-\d{2})', os.path.basename(value)):
        M = value.split('.')
        date_object = datetime.datetime.strptime(M[2], "%Y-%m-%d").date()
        if date_object != yesterday.date():
            continue;
        with open(value, 'r') as f:
            for line in f.readlines():
                # if line.__contains__("create player success"):
                if re.search(r'create player success', line):
                    analysis_content['analysis_create_new_character'].append(
                        create_player_info(line.strip(), 'create_new_character'))
                if re.search(r'login success', line):
                    login_info = login_player_info(line.strip(), 'daily_login')
                    analysis_content['analysis_daily_login'][login_info['playerId']] = login_info
                if re.search(r'StatGameResult ', line):
                    match_info = get_match_info(line.strip(), 'match')
                    if not analysis_content['analysis_daily_matches'].has_key(match_info['playerId']):
                        analysis_content['analysis_daily_matches'][match_info['playerId']] = []
                    analysis_content['analysis_daily_matches'][match_info['playerId']].append(match_info)
                if re.search(r'addCurrency', line):
                    analysis_content['analysis_produce'].append(output_info(line.strip(), 'produce'))
                if re.search(r'addItem', line):
                    analysis_content['analysis_produce'].append(output_info(line.strip(), 'produce', pattern='item'))
                if re.search(r'subCurrency', line):
                    analysis_content['analysis_consumption'].append(output_info(line.strip(), 'consumption'))
                if re.search(r'subItem', line):
                    analysis_content['analysis_produce'].append(
                        output_info(line.strip(), 'consumption', pattern='item'))
                if re.search(r'confirmOrder,', line):
                    analysis_content['analysis_payment'].append(payment_info(line.strip(), 'payment'))

for key in analysis_content:
    collection_name = 'log_date.' + yesterday_date_string
    collection = db[collection_name]
    if isinstance(analysis_content[key], dict):
        for v in analysis_content[key]:
            if isinstance(analysis_content[key][v], list):
                m = {'type': 'match', 'playerId': v, 'matches': analysis_content[key][v]}
            else:
                m = analysis_content[key][v]
                collection.insert_one(m)
    else:
        for v in analysis_content[key]:
            collection.insert_one(v)
            # if not collection.find_one({'create_date': now_date_string}):
            #     collection.insert_one({
            #         'create_date': now_date_string,
            #         'log_date':yesterday_date_string,
            #         'analysis_content': analysis_content[key]
            #     })
