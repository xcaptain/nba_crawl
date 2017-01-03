# coding: utf-8

from nba_py import player
from nba_py import league
import yaml
import json
from sqlalchemy import create_engine
from sqlalchemy import text
import requests

with open("config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)


class PlayerCareerSyncer:
    def __init__(self):
        host = cfg['mysql']['host']
        user = cfg['mysql']['user']
        passwd = cfg['mysql']['passwd']
        db = cfg['mysql']['db']
        dsn = "mysql+pymysql://" + user + ":" + passwd + "@" + host + "/" + db + '?charset=utf8'
        engine = create_engine(dsn, encoding='utf8', echo=True)
        engine.execute(
            text('DROP TABLE IF EXISTS player_career_regular_season;'))
        engine.execute(text('DROP TABLE IF EXISTS player_summary;'))
        self.player_translations = self.get_player_translation()
        self.conn = engine.connect()

    def sync_all(self):
        all_player_id = player.PlayerList().info().loc[:, 'PERSON_ID']
        for player_id in all_player_id:
            self.sync_one(player_id)
        self.sync_game_log()

    def sync_one(self, player_id):
        self.sync_summary(player_id)
        self.sync_career(player_id)

    def sync_summary(self, player_id):
        summary = player.PlayerSummary(player_id).info()
        translation = self.player_translations.get(player_id, '')
        if translation:
            summary.loc[:, 'CHINESE_NAME'] = translation['CHINESE_NAME']
            summary.loc[:, 'CHINESE_POSITION'] = translation['CHINESE_POSITION']
            summary.loc[:, 'CHINESE_COUNTRY'] = translation['CHINESE_COUNTRY']
        summary.to_sql('player_summary', self.conn, if_exists='append')

    def sync_career(self, player_id):
        player.PlayerCareer(player_id).regular_season_totals().to_sql(
            'player_career_regular_season', self.conn, if_exists='append')

    def sync_user_profile(self, player_id):
        player.PlayerProfile(player_id).career_highs().to_sql(
            'player_profiles', self.conn, if_exists='append')

    def sync_game_log(self):
        league.GameLog().overall().to_sql(
            'league_game_logs', self.conn, if_exists='replace')

    def get_player_translation(self):
        url = 'http://china.nba.com/static/data/league/playerlist.json'
        d = json.loads(requests.get(url).text)
        m = {}
        for item in d['payload']['players']:
            profile = item['playerProfile']
            player_id = int(profile['playerId'])
            info = {
                'PERSON_ID': player_id,
                'CHINESE_POSITION': profile['position'],
                'CHINESE_NAME': profile['displayName'],
                'CHINESE_COUNTRY': profile['country'],
            }
            m[player_id] = info
        return m


if __name__ == '__main__':
    o = PlayerCareerSyncer()
    o.sync_all()
    # o.sync_game_log()
    # o.sync_schedule()
    # o.sync_summary(1713)
