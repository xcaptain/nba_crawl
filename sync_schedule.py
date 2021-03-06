import requests
import pandas as pd
from nba_py import game
import yaml
import json
from sqlalchemy import create_engine
from sqlalchemy import text
import time
import datetime
import os

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.yml'), 'r') as ymlfile:
    cfg = yaml.load(ymlfile)


class ScheduleSyncer:
    def __init__(self):
        self.url = 'http://data.nba.com/data/10s/v2015/json/mobile_teams/nba/2016/league/00_full_schedule.json'
        host = cfg['mysql']['host']
        user = cfg['mysql']['user']
        passwd = cfg['mysql']['passwd']
        db = cfg['mysql']['db']
        dsn = "mysql+pymysql://" + user + ":" + passwd + "@" + host + "/" + db
        self.engine = create_engine(dsn, encoding='utf8', echo=True)
        self.conn = self.engine.connect()
        self.df = None

    def sync_all(self):
        '''全量更新'''
        self.engine.execute(text('DROP TABLE IF EXISTS matches;'))
        self.engine.execute(text('DROP TABLE IF EXISTS match_detail;'))
        self.schedule_list = self.get_schedule_list()
        self.sync_summary()
        self.sync_detail_all()

    def sync_incremental(self):
        '''增量更新'''
        self.engine.execute(text('DROP TABLE IF EXISTS matches;'))
        self.schedule_list = self.get_schedule_list()
        self.sync_summary()
        self.sync_detail_incremental()

    def sync_summary(self):
        df = pd.read_json(json.dumps(self.schedule_list))
        df.to_sql('matches', self.conn, if_exists='replace')

    def sync_detail_all(self):
        for item in self.schedule_list:
            gdate = item.get('gdte')
            t1 = time.strptime(gdate, '%Y-%m-%d')  # east time
            t2 = time.localtime()  # current time
            if t1 > t2:  # avoid overwhelming
                break
            gid = '{0:010d}'.format(int(item.get('gid')))
            game.Boxscore(gid).team_stats().to_sql(
                'match_detail', self.conn, if_exists='append')

    def sync_detail_incremental(self):
        gids = []
        for item in self.schedule_list:
            gdate = item.get('gdte')
            t1 = datetime.datetime.now() - datetime.timedelta(7) # 增加备份时间
            t2 = datetime.datetime.strptime(gdate, '%Y-%m-%d')
            t3 = datetime.datetime.now() + datetime.timedelta(1) # 北京时间比西方时间早
            if t1 < t2 and t2 < t3:
                gid = '{0:010d}'.format(int(item.get('gid')))
                gids.append(gid)
        for gid in gids:
            game.Boxscore(gid).team_stats().to_sql('match_detail', self.conn, if_exists='append')

    def get_schedule_list(self):
        r = requests.get(self.url).text
        d = json.loads(r)
        l = []
        for item in d['lscd']:
            t = item['mscd']['g']
            for item2 in t:
                item2.pop('bd')
                item2['visit_team'] = item2.get('v').get('tid')
                item2['host_team'] = item2.get('h').get('tid')
                item2.pop('v')
                item2.pop('h')
                item2.pop('is')
                item2.pop('seri')
                item2.pop('ppdst')
                item2.pop('seq')
                item2.pop('st')
                item2.pop('stt')
                if (item2.get('ppd')):
                    item2.pop('ppd')
                if (item2.get('ptsls')):
                    lose = item2.get('ptsls')
                    loseOne = lose.get('pl')[0]
                    item2['lose'] = loseOne.get('tid')
                    item2.pop('ptsls')
                l.append(item2)

        return l


if __name__ == '__main__':
    o = ScheduleSyncer()
    o.sync_incremental()
    # o.sync_all()
