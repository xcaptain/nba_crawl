from nba_py import player
from nba_py import league
import yaml
from sqlalchemy import create_engine

with open("config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

class PlayerCareerSyncer:
    def __init__(self):
        host = cfg['mysql']['host']
        user = cfg['mysql']['user']
        passwd = cfg['mysql']['passwd']
        db = cfg['mysql']['db']
        dsn = "mysql+pymysql://"+user+":"+passwd+"@"+host+"/"+db
        engine = create_engine(dsn, encoding='utf8', echo=True)
        self.conn = engine.connect()

    def sync_all(self):
        all_player_id = player.PlayerList().info().loc[:,'PERSON_ID']
        for player_id in all_player_id:
            # self.sync_one(player_id)
            # self.sync_user_profile(player_id)
            self.sync_summary(player_id)

    def sync_one(self, player_id):
        player.PlayerCareer(player_id).regular_season_totals().to_sql('player_career_regular_season', self.conn, if_exists='append')
        # player.PlayerGameLogs(player_id=player_id).info().to_sql('player_game_logs', self.conn, if_exists='append')

    def sync_summary(self, player_id):
        player.PlayerSummary(player_id).info().to_sql('player_summary', self.conn, if_exists='append')

    def sync_user_profile(self, player_id):
        player.PlayerProfile(player_id).career_highs().to_sql('player_profiles', self.conn, if_exists='append')

    def sync_schedule(self):
        league.GameLog().overall().to_sql('league_game_logs', self.conn, if_exists='append')

if __name__ == '__main__':
    o = PlayerCareerSyncer()
    o.sync_all()
    # o.sync_schedule()
