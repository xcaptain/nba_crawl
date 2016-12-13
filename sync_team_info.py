from nba_py import team
import yaml
from sqlalchemy import create_engine

with open("config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

class TeamSyncer:
    def __init__(self):
        host = cfg['mysql']['host']
        user = cfg['mysql']['user']
        passwd = cfg['mysql']['passwd']
        db = cfg['mysql']['db']
        dsn = "mysql+pymysql://"+user+":"+passwd+"@"+host+"/"+db
        engine = create_engine(dsn, encoding='utf8', echo=True)
        self.conn = engine.connect()


    def sync_all(self):
        ts = team.TeamList().info().loc[:,'TEAM_ID']
        for team_id in ts:
            self.sync_one(team_id)

    def sync_one(self, team_id):
        team.TeamSummary(team_id).info().to_sql('team_summary', self.conn, if_exists='append')

if __name__ == '__main__':
    o = TeamSyncer()
    o.sync_all()
