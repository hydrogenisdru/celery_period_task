from celery.schedules import crontab
from celery.utils.log import get_task_logger
import os, re, datetime, sys
import demjson
from pymongo import MongoClient
import requests
import redis

logger = get_task_logger(__name__)


def init_mongo_db(host='127.0.0.1', port=27027, db_name=['analysis', 'fire2']):
    mongo_client = MongoClient(host=host, port=port, connect=False)
    return mongo_client[db_name[0]], mongo_client[db_name[1]]


def init_redis(host='localhost', port=6779, index=14):
    pool = redis.ConnectionPool(host=host, port=port, db=index)
    rdb = redis.Redis(connection_pool=pool)
    return rdb


from celery import Celery

app = Celery('task', backend='mongodb://localhost:27027', broker='redis://localhost:6779/1')
db, game_db = init_mongo_db()
rdb = init_redis()


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(30.0, test.s('world'), expires=10)
    sender.add_periodic_task(crontab(hour=0, minute=0), daily_game_server_log_analysis())


@app.task
def add(x, y):
    result = x + y
    print result
    return result


@app.task
def test(arg):
    collection = db['heartbeat']
    collection.insert_one({
        'createAt': datetime.datetime.now(),
        'content': arg})


@app.task
def days_retention():
    for days in [2, 4, 8, 16, 31]:
        retention(days=days)


@app.task
def daily_match_reward(days=1):
    collect_date = datetime.datetime.now() - datetime.timedelta(days=days)
    collect_date_string = datetime.datetime.strftime(collect_date, "%Y-%m-%d")
    collection_name = 'log_date.' + collect_date_string
    match_infos = list(db[collection_name].find({'type': 'match'}))
    toWhoms = short_match_info_list(match_infos, short_length=20)
    for toWhom in toWhoms:
        reward_mail = {
            'template': 0,
            'content': {'0': 11},
            'params': ['daily match reward'],
            'createAt': datetime.datetime.utcnow(),
            'toWhichZone': [],
            'toWhom': toWhom
        }
        game_db.systemMail.insert_one(reward_mail)


@app.task
def xmas_battle_reward():
    collect_dates = ["2017-12-23", "2017-12-24"]
    toWhom = set()
    for collect_date in collect_dates:
        collect_name = 'log_date.' + collect_date
        match_infos = list(db[collect_name].find({'type': 'match'}))
        lucky_filter(match_infos=match_infos, toWhom=toWhom)
    reward_mail = {
        'template': 6,
        'content': {'50003': 1},
        'params': ["恭喜您在活动中达到了奖励条件，特此献上头像框“冬季恋歌”，祝您节日愉快~"],
        'createAt': datetime.datetime.utcnow(),
        'toWhichZone': [],
        'toWhom': toWhom
    }
    game_db.systemMail.insert_one(reward_mail)


def lucky_filter(match_infos=[], toWhom=set()):
    for info in match_infos:
        cnt = 0
        for match in info['matches']:
            if match['result'] == 1:
                cnt += 1
        if cnt > 1:
            toWhom.add(info['playerId'])


@app.task
def daily_rank_reward(days=1):
    collect_date = datetime.datetime.now() - datetime.timedelta(days=days)
    collect_date_string = datetime.datetime.strftime(collect_date, "%Y-%m-%d")
    collection_name = 'log_date.' + collect_date_string
    match_infos = list(db[collection_name].find({'type': 'match'}))
    toWhom = []
    for m in match_infos:
        cnt = 0
        for info in m:
            finished_time = datetime.datetime.strptime(info['finishedTime'], '%Y-%m-%d %H:%M:%S')
            begin_time = datetime.datetime.strptime(collect_date_string, '%Y-%m-%d') + datetime.timedelta(hours=19)
            end_time = begin_time + datetime.timedelta(hours=3)
            if begin_time <= finished_time <= end_time and info['type'] == 'ranking':
                cnt += 1
            if cnt > 2:
                toWhom.append(info['playerId'])
    reward_mail = {
        'template': 6,
        'content': {'170': 3},
        'params': ['尊敬的枪神，您昨天在活动期间已经完成了3场天梯比赛，这是您的奖励，请再接再厉继续征战。'],
        'createAt': datetime.datetime.utcnow(),
        'toWhichZone': [],
        'toWhom': toWhom
    }
    game_db.systemMail.insert_one(reward_mail)


@app.task
def daily_login_reward(days=1):
    collect_date = datetime.datetime.now() - datetime.timedelta(days=days)
    collect_date_string = datetime.datetime.strftime(collect_date, "%Y-%m-%d")
    collection_name = 'log_date.' + collect_date_string
    login_infos = list(db[collection_name].find({'type': 'daily_login'}))
    toWhom = []
    for d in login_infos:
        toWhom.append(d['playerId'])
    reward_mail = {
        'template': 6,
        'content': {'170': 1},
        'params': ['每日限时登陆奖励。'],
        'createAt': datetime.datetime.utcnow(),
        'toWhichZone': [],
        'toWhom': toWhom
    }
    game_db.systemMail.insert_one(reward_mail)


@app.task
def retention_single(begin_date, days=2):
    retention_s(begin_date, days)


def short_match_info_list(match_infos, short_length=50):
    players = []
    for i in range(0, match_infos.__len__()):
        if match_infos[i]['matches'].__len__() > 0:
            players.append(long(match_infos[i]['playerId']))
        if i % short_length == short_length - 1:
            yield players
            players = []
        if match_infos.__len__() % short_length and i == match_infos.__len__() - 1:
            yield players


@app.task
def daily_game_server_log_analysis(days=1):
    now = datetime.datetime.now()
    now_date_string = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
    yesterday = now - datetime.timedelta(days=days)
    yesterday_date_string = datetime.datetime.strftime(yesterday, "%Y-%m-%d")
    analysis_content = dict()
    analysis_content['analysis_create_new_character'] = []
    analysis_content['analysis_daily_login'] = dict()
    analysis_content['analysis_produce'] = []
    analysis_content['analysis_consumption'] = []
    analysis_content['analysis_payment'] = []
    analysis_content['analysis_daily_matches'] = dict()
    os.chdir("/data/server/fire2/log/")
    L = []
    for files in os.walk("/data/server/fire2/log/"):
        for file in files:
            L.append(file)

    for value in L[2]:
        if re.match(r'GameServer.log(.*\d{4}\-\d{2}\-\d{2})', os.path.basename(value)):
            M = value.split('.')
            date_object = datetime.datetime.strptime(M[2], "%Y-%m-%d").date()
            if date_object != yesterday.date():
                continue;
            with open(value, 'r') as f:
                for line in f.readlines():
                    # if line.__contains__("create player success"):
                    if re.search(r'create player success', line):
                        create_info = create_player_info(line.strip(), 'create_new_character')
                        analysis_content['analysis_create_new_character'].append(create_info)
                        analysis_content['analysis_daily_login'][create_info['playerId']] = \
                            format_login_info('daily_login', create_info['create_datetime'], create_info['playerId'])
                    if re.search(r'login success', line):
                        login_info = login_player_info(line.strip(), 'daily_login')
                        analysis_content['analysis_daily_login'][login_info['playerId']] = login_info
                    if re.search(r'loginBySessionToken success', line):
                        login_info = login_player_info(line.strip(), 'daily_login')
                        analysis_content['analysis_daily_login'][login_info['playerId']] = login_info
                    if re.search(r'StatGameResult ', line):
                        match_info = get_match_info(line.strip(), 'match')
                        if not analysis_content['analysis_daily_matches'].has_key(match_info['playerId']):
                            analysis_content['analysis_daily_matches'][match_info['playerId']] = []
                        analysis_content['analysis_daily_matches'][match_info['playerId']].append(match_info)
                    if re.search(r'addCurrency ', line):
                        analysis_content['analysis_produce'].append(output_info(line.strip(), 'produce'))
                    if re.search(r'addItem ', line):
                        analysis_content['analysis_produce'].append(
                            output_info(line.strip(), 'produce', pattern='item'))
                    if re.search(r'subCurrency', line):
                        analysis_content['analysis_consumption'].append(output_info(line.strip(), 'consumption'))
                    if re.search(r'subItem', line):
                        analysis_content['analysis_produce'].append(
                            output_info(line.strip(), 'consumption', pattern='item'))
                    if re.search(r'confirmOrder', line):
                        analysis_content['analysis_payment'].append(payment_info(line.strip(), 'payment'))

            for key in analysis_content:
                collection_name = 'log_date.' + yesterday_date_string
                collection = db[collection_name]
                if isinstance(analysis_content[key], dict):
                    for v in analysis_content[key]:
                        collection.insert_one(analysis_content[key][v])
                else:
                    for v in analysis_content[key]:
                        collection.insert_one(v)
                        # now = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
                        # analysis_content = dict()
                        # analysis_content['analysis_create_new_character'] = []
                        # analysis_content['analysis_daily_login'] = dict()
                        # analysis_content['analysis_produce'] = []
                        # analysis_content['analysis_consumption'] = []
                        # analysis_content['analysis_payment'] = []
                        # os.chdir("/data/server/fire2/log/")
                        # L = []
                        # for files in os.walk("/data/server/fire2/log/"):
                        #     for file in files:
                        #         L.append(file)
                        #
                        # for value in L[2]:
                        #     if re.match(r'^GameServer.log$', os.path.basename(value)):
                        #         with open(value, 'r') as f:
                        #             for line in f.readlines():
                        #                 # if line.__contains__("create player success"):
                        #                 if re.search(r'create player success', line):
                        #                     analysis_content['analysis_create_new_character'].append(create_player_info(line.strip()))
                        #                 if re.search(r'login success', line):
                        #                     login_info = login_player_info(line.strip())
                        #                     analysis_content['analysis_daily_login'][login_info['playerId']] = login_info
                        #                 if re.search(r'addCurrency', line):
                        #                     analysis_content['analysis_produce'].append(output_info(line.strip()))
                        #                 if re.search(r'addItem', line):
                        #                     analysis_content['analysis_produce'].append(output_info(line.strip(), pattern='item'))
                        #                 if re.search(r'subCurrency', line):
                        #                     analysis_content['analysis_consumption'].append(output_info(line.strip()))
                        #                 if re.search(r'subItem', line):
                        #                     analysis_content['analysis_produce'].append(output_info(line.strip(), pattern='item'))
                        #                 if re.search(r'confirmOrder', line):
                        #                     analysis_content['analysis_payment'].append(payment_info(line.strip()))
                        #
                        #         for key in analysis_content:
                        #             collection = db[key]
                        #             if not collection.find_one({'create_date': now}):
                        #                 collection.insert_one({
                        #                     'create_date': now,
                        #                     'analysis_content': analysis_content[key]
                        #                 })
                        # save_file(key, now, demjson.encode(analysis_content[key], encoding='utf-8'))


@app.task
def population_log():
    return None


@app.task
def white_list_trigger(trigger=False):
    url = 'http://123.59.66.147:8086/test/setWhiteListTrigger'
    params = {
        'isOpen': trigger
    }
    result = {'code': 1000, 'message': 'request failed', 'content': ''}
    r = requests.get(url=url, params=params)
    if r.status_code == 200:
        result = demjson.decode(r.text)
        if result['code'] == 0:
            result['code'] = 0
            result['message'] = 'success'
            result['content'] = r.text
    print demjson.encode(result, encoding='utf-8')


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
    info['trade_no'] = re.search(r'trade_no: \d+', line_txt).group().split(" ")[1].strip()
    info['goodsId'] = re.search(r'goodsId: .*rmb', line_txt).group().split(" ")[1].strip()
    info['rmb'] = re.search(r'rmb: \d+', line_txt).group().split(" ")[1].strip()
    return info


def login_player_info(line_txt, type):
    info = dict()
    info['type'] = type
    info['login_datetime'] = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line_txt).group().strip()
    info['playerId'] = re.search(r'playerId:\d{5,}', line_txt).group().split(":")[1].strip()
    return info


def get_match_info(line_txt, type):
    info = dict()
    info['type'] = type
    info['playerId'] = re.search(r'playerId:\d{5,}', line_txt).group().strip()
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
    info['expAdd'] = re.search(r'expAdd:\d+', line_txt).group().split(":")[1].strip()
    info['gameTime'] = re.search(r'gameTime:\d+', line_txt).group().split(":")[1].strip()
    info['activeTime'] = re.search(r'activeTime:\d+', line_txt).group().split(":")[1].strip()
    return info


def format_login_info(type, login_datetime, playerId):
    info = dict()
    info['type'] = type
    info['login_datetime'] = login_datetime
    info['playerId'] = playerId
    return info


def create_player_info(line_txt, type):
    info = dict()
    info['type'] = type
    info['create_datetime'] = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line_txt).group().strip()
    info['playerId'] = re.search(r'\d{5,}', line_txt).group().strip()
    info['nick'] = re.search(r'nick:.*', line_txt).group().split(" ")[1].strip()
    return info


def daily_new_create_battle_summary(days=2):
    start_date = datetime.datetime.now() - datetime.timedelta(days=days)
    start_date_string = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    start_collection_name = 'log_date.' + start_date_string
    new_create_battle_cnt = dict()
    for info in list(db[start_collection_name].find({'type': 'create_new_character'})):
        check_date_string = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
        check_collection_name = 'log_date.' + check_date_string
        battle_info = db[check_collection_name].find_one(
            {'type': 'match', 'playerId': info['playerId']})
        if battle_info:
            if new_create_battle_cnt.has_key(battle_info['matches'].__len__()):
                new_create_battle_cnt[battle_info['matches'].__len__()] += 1
            else:
                new_create_battle_cnt[battle_info['matches'].__len__()] = 1



def retention(days=2):
    start_date = datetime.datetime.now() - datetime.timedelta(days=days)
    start_date_string = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    start_collection_name = 'log_date.' + start_date_string
    start_create_collection = list(db[start_collection_name].find({'type': 'create_new_character'}))
    retention_people = dict()
    for player in start_create_collection:
        retention_people[player['playerId']] = False
    # for k in range(1, days):
    end_date = start_date + datetime.timedelta(days=days - 1)
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
        'start_date': start_date_string,
        'end_date': end_date_string,
        'retention_type': days - 1,
        'retention_people': r,
        'retention_rate': float('%.2f' % (r * 100.0 / start_create_collection.__len__()))
    })


def retention_s(begin_date, days=1):
    start_date = datetime.datetime.strptime(begin_date, '%Y-%m-%d %H:%M:%S')
    start_date_string = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    start_collection_name = 'log_date.' + start_date_string
    start_create_collection = list(db[start_collection_name].find({'type': 'create_new_character'}))
    retention_people = dict()
    for player in start_create_collection:
        retention_people[player['playerId']] = False
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
        'start_date': start_date_string,
        'end_date': end_date_string,
        'retention_type': days,
        'retention_people': r,
        'retention_rate': float('%.2f' % (r * 100.0 / start_create_collection.__len__()))
    })


def collect_paying_player(days=2):
    for i in range(1, days):
        collect_date = datetime.datetime.now() - datetime.timedelta(days=i)
        collect_date_string = datetime.datetime.strftime(collect_date, "%Y-%m-%d")
        collection_name = 'log_date.' + collect_date_string
        paying_players = list(db[collection_name].find({'type': 'payment'}))
        if paying_players:
            for info in paying_players:
                paying_player = db['paying_player'].find({'playerId': info['playerId']})
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


def paying_player_retention(days=2):
    start_date = datetime.datetime.now() - datetime.timedelta(days=days)
    start_date_string = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    paying_players = list(db['paying_player'].find({'first_pay_date': start_date_string}))
    retention_people = dict()
    for info in paying_players:
        retention_people[info['playerId']] = False
    end_date = start_date + datetime.timedelta(days=days - 1)
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
    db['paying_player_retention'].insert_one({
        'createAt': datetime.datetime.now(),
        'start_date': start_date_string,
        'end_date': end_date_string,
        'retention_type': days - 1,
        'retention_people': r,
        'retention_rate': float('%.2f' % (r * 100.0 / paying_players.__len__()))
    })


def paying_player_retention_s(begin_date, days=2):
    start_date = datetime.datetime.strptime(begin_date, '%Y-%m-%d %H:%M:%S')
    start_date_string = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    paying_players = list(db['paying_player'].find({'first_pay_date': start_date_string}))
    retention_people = dict()
    for info in paying_players:
        retention_people[info['playerId']] = False
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
    db['paying_player_retention'].insert_one({
        'createAt': datetime.datetime.now(),
        'start_date': start_date_string,
        'end_date': end_date_string,
        'retention_type': days - 1,
        'retention_people': r,
        'retention_rate': float('%.2f' % (r * 100.0 / paying_players.__len__()))
    })


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
