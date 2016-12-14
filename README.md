# nba_crawl
通过[https://github.com/seemethere/nba_py](https://github.com/seemethere/nba_py) 提供的接口获取nba信息

### 依赖项：
1. pip3 install nba_py
2. pip3 install sqlalchemy
3. pip3 install pymysql
4. pip3 install pandas
5. pip3 install requests

### 创建数据库:
1. create database test;

2. GRANT ALL PRIVILEGES ON test . * TO 'user@'localhost';

3. FLUSH PRIVILEGES;

### 运行
1. python3 sync_player_info.py
2. python3 sync_team_info.py
