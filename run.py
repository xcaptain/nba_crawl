from sync_player_info import PlayerCareerSyncer
from sync_team_info import TeamSyncer
from sync_schedule import ScheduleSyncer

print('start sync player info')
playerSyncer = PlayerCareerSyncer()
playerSyncer.sync_all()

print('start sync team info')
teamSyncer = TeamSyncer()
teamSyncer.sync_all()

print('start sync schedule info')
scheduleSyncer = ScheduleSyncer()
scheduleSyncer.sync_all()

print('all sync done')
