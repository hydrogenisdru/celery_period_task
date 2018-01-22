# coding=utf-8
from pymongo import MongoClient
from bson import DBRef
from models import init_db, PlayerInfo


def init_mongo_db(db_name=['fire2']):
    mongo_client = MongoClient(host="123.59.71.187", port=27027, connect=False)
    return mongo_client[db_name[0]]


def init_game_db(db_name=['fire2']):
    mongo_client = MongoClient(host="120.92.216.136", port=27027, connect=False)
    # mongo_client = MongoClient(host="123.59.71.187", port=27027, connect=False)
    return mongo_client[db_name[0]]


production_db = init_game_db()
mirror_db = init_mongo_db()


def copy_account(raw_account_id, mirror_account_id):
    raw_player = production_db['player'].find_one({'playerId': raw_account_id})
    if raw_player:
        mirror_db['player'].insert(raw_player)
    for key in raw_player:
        if isinstance(raw_player[key], DBRef):
            print raw_player[key].collection
            p = production_db[raw_player[key].collection].find_one({'playerId': raw_account_id})
            if p:
                mirror_db[raw_player[key].collection].insert(p)
    session = init_db()()
    k = session.query(PlayerInfo).filter(PlayerInfo.uuid == mirror_account_id)
    if k:
        for info in k:
            info.uuid = raw_account_id
        session.commit()
    else:
        print 'no uuid ' + str(raw_account_id)
    session.close()

    # mirror_db['gun'].update({'playerId': mirror_account_id},
    #                         {
    #                             '$set': {
    #                                 'guns': raw_gun['guns']
    #                             }
    #                         })
    # mirror_db['gunSkin'].update({'playerId': mirror_account_id},
    #                             {
    #                                 '$set': {
    #                                     'skins': raw_gunSkin['skins']
    #                                 }
    #                             })
    # mirror_db['hero'].update({'playerId': mirror_account_id},
    #                          {
    #                              '$set': {
    #                                  'heros': raw_hero['heros']
    #                              }
    #                          })
    # mirror_db['heroSkin'].update({'playerId': mirror_account_id},
    #                              {
    #                                  '$set': {
    #                                      'skins': raw_heroSkin['skins']
    #                                  }
    #                              })


copy_account(raw_account_id=12100519552817, mirror_account_id=12115808723793)
