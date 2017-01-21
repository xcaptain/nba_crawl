from nba_py import team
import yaml
from sqlalchemy import create_engine
from sqlalchemy import text
import json
import requests
import os

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.yml'), 'r') as ymlfile:
    cfg = yaml.load(ymlfile)


class TeamSyncer:
    def __init__(self):
        host = cfg['mysql']['host']
        user = cfg['mysql']['user']
        passwd = cfg['mysql']['passwd']
        db = cfg['mysql']['db']
        dsn = "mysql+pymysql://" + user + ":" + passwd + "@" + host + "/" + db + '?charset=utf8'
        engine = create_engine(dsn, encoding='utf8', echo=True)
        engine.execute(text('DROP TABLE IF EXISTS team_summary;'))
        self.team_translations = self.get_team_translation()
        self.conn = engine.connect()

    def sync_all(self):
        ts = team.TeamList().info().loc[:, 'TEAM_ID']
        for team_id in ts:
            self.sync_one(team_id)

    def sync_one(self, team_id):
        team_id = int(team_id)
        summary = team.TeamSummary(team_id).info()
        translation = self.team_translations.get(team_id)
        if (translation):
            summary.loc[:, 'CHINESE_NAME'] = translation['CHINESE_NAME']
            summary.loc[:, 'CHINESE_CITY'] = translation['CHINESE_CITY']
            summary.to_sql('team_summary', self.conn, if_exists='append')

    def get_team_translation(self):
        url = 'http://china.nba.com/static/data/league/playerlist.json'
        d = json.loads(requests.get(url).text)
        m = {}
        for item in d['payload']['players']:
            profile = item['teamProfile']
            team_id = int(profile['id'])
            info = {
                'TEAM_ID': team_id,
                'CHINESE_NAME': profile['name'],
                'CHINESE_CITY': profile['city'],
            }
            if not m.get(team_id):
                m[team_id] = info

        return m


if __name__ == '__main__':
    o = TeamSyncer()
    o.sync_all()
