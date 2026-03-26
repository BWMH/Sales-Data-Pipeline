from nba_api.stats.endpoints import boxscoretraditionalv3

box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id="0022400001")
print(box.get_available_data())
print(box.get_data_frames()[0].columns.tolist())