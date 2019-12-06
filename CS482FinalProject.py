
import mysql.connector as sqlcon
import pandas as pd
import string
import random
import csv
from random import randint
import time
import contextlib
import matplotlib.pyplot as plt

from ipywidgets import widgets as w
from IPython.display import display


# Team choices

NFL_teams=('Cardinals', 'Falcons', 'Ravens', 'Bills', 'Panthers', 'Bears','Bengals', 'Browns', 'Cowboys', 'Broncos', 'Lions', 'Packers','Texans','Colts', 'Jaguars', 'Chiefs', 'Dolphins', 'Vikings','Patriots', 'Saints', 'Giants', 'Jets', 'Raiders', 'Eagles','Steelers', 'Rams', 'Chargers', '49ers', 'Seahawks','Buccaneers','Titans','Redskins')

NFL_stadiums=('Arrowhead', 'AT&T', 'Bank of America', 'Broncos', 'CenturyLink Field', 'FedExField', 'FirstEnergy','Ford Field', 'Gillette', 'Hard Rock', 'Heinz Field', 'Lambeau Field')

NFL_city=('Arizona', 'Atlanta', 'Baltimore', 'Buffalo', 'Carolina', 'Chicago','Cincinnati', 'Cleveland', 'Dallas', 'Denver', 'Detroit','Houston','Indianapolis','Jacksonville', 'Kansas', 'Miami', 'Minnesota','New England', 'New Orleans','New York', 'New York Jets', 'Oakland Raiders', 'Philadelphia','Pittsburgh','San Diego', 'San Francisco', 'Seattle','Tampa Bay','Tennessee','Washington')




# Functions

def random_date():
    year = randint(2000, 2019)
    month = randint(1, 12)
    if(month<10):
        month='0'+str(month)
    day = randint(1, 28)
    if(day<10):
        day='0'+str(day)
    return str(year)+str(month)+str(day)




def get_TeamIDs():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    cursor.execute("SELECT TeamID FROM teams")
    result = cursor.fetchall()
    db.close()
    return [item[0] for item in result]

def get_PlayerIDs():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    cursor.execute("SELECT PlayerID FROM Players")
    result = cursor.fetchall()
    db.close()
    return [item[0] for item in result]

def get_GameIDs():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    cursor.execute("SELECT GameID FROM Games")
    result = cursor.fetchall()
    db.close()
    return [item[0] for item in result]




def delete_players():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    sql = "DELETE FROM players"
    cursor.execute(sql)
    db.commit()
    db.close()
    print("players table cleared")

def delete_teams():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    sql = "DELETE FROM teams"
    cursor.execute(sql)
    db.commit()
    db.close()
    print("teams table cleared")

def delete_games():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    sql = "DELETE FROM Games"
    cursor.execute(sql)
    db.commit()
    db.close()
    print("games table cleared")

def delete_play():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    sql = "DELETE FROM Play"
    cursor.execute(sql)
    db.commit()
    db.close()
    print("play table cleared")




def insert_teams(file_name='teams.csv'):
    vals=[]
    with open(file_name) as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            vals.append(tuple(row))
        db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
        cursor = db.cursor()
        start = time.time()
        for row in vals:
            sql = "INSERT INTO teams (TeamID, TeamName, city) VALUES (%s, %s, %s)"
            
            try:
                cursor.execute(sql, row)
            except sqlcon.Error as e:
                return e.msg
        db.commit()
        db.close()
        end = time.time()
        print(end - start, 'seconds for single insert teams')
        s=str(end - start) + 'seconds'
        return s





def multi_row_teams(file_name='teams.csv'):
    df = pd.read_csv(file_name, sep=',', header=None)
    cols = str(df.columns.values.tolist()).replace('[','').replace(']', '')
    values = list(map(tuple, df.values))
    half = len(values) // 2
    valuesA = values[:half]
    valuesB = values[half:]
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    start = time.time()
    
    sql = "insert into teams ( TeamID, TeamName, city ) values  (%s, %s, %s );" 
    try:
        cursor.executemany(sql, valuesA)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    try:
        cursor.executemany(sql, valuesB)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    db.close()
    end = time.time()
    print(end - start, 'seconds for multi row teams')
    s=str(end - start) + 'seconds'
    return s





def bulk_teams(file_name='teams.csv'):
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password',
                        allow_local_infile=True)
    cursor = db.cursor()
    start = time.time()
    sql = "LOAD DATA LOCAL INFILE \'%s\' INTO TABLE teams FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' (TeamID, TeamName, city)" % (file_name)
    try:
        cursor.execute(sql)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    db.close()
    end = time.time()
    print(end - start, 'seconds for bulk teams')
    s=str(end - start) + 'seconds'
    return s





def insert_players(file_name='players.csv'):
    vals=[]
    with open(file_name) as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            vals.append(tuple(row))
        db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
        cursor = db.cursor()
        start = time.time()
        for row in vals:
            sql = "INSERT INTO players (PlayerID, TeamID, FirstName, LastName, Position, Touchdowns, TotalYards, Salary) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            try:
                cursor.execute(sql, row)
            except sqlcon.Error as e:
                return e.msg
        db.commit()
        db.close()
        end = time.time()
        print(end - start, 'seconds for single insert players')
        s=str(end - start) + 'seconds'
        return s





def multi_row_players(file_name='players.csv'):
    df = pd.read_csv(file_name, sep=',', header=None)
    cols = str(df.columns.values.tolist()).replace('[','').replace(']', '')
    values = list(map(tuple, df.values))
    half = len(values) // 2
    valuesA = values[:half]
    valuesB = values[half:]
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    start = time.time()
    
    sql = "INSERT INTO players (PlayerID, TeamID, FirstName, LastName, Position, Touchdowns, TotalYards, Salary) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
    try:
        cursor.executemany(sql, valuesA)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    try:
        cursor.executemany(sql, valuesB)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    db.close()
    end = time.time()
    print(end - start, 'seconds for multi row players')
    s=str(end - start) + 'seconds'
    return s





def bulk_players(file_name='players.csv'):
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password',
                        allow_local_infile=True)
    cursor = db.cursor()
    start = time.time()
    sql = "LOAD DATA LOCAL INFILE \'%s\' INTO TABLE players FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' (PlayerID, TeamID, FirstName, LastName, Position, Touchdowns, TotalYards, Salary)" % (file_name)
    try:
        cursor.execute(sql)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    db.close()
    end = time.time()
    print(end - start, 'seconds for bulk players')
    s=str(end - start) + 'seconds'
    return s





def insert_games(file_name='games.csv'):
    vals=[]
    with open(file_name) as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            vals.append(tuple(row))
        db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
        cursor = db.cursor()
        start = time.time()
        for row in vals:
            sql = "INSERT INTO Games (GameID, Date, Stadium, Result, Attendance, TicketRevenue) VALUES (%s, %s, %s, %s, %s, %s)"
            try:
                cursor.execute(sql, row)
            except sqlcon.Error as e:
                return e.msg
        db.commit()
        db.close()
        end = time.time()
        print(end - start, 'seconds for single insert games')
        s=str(end - start) + 'seconds for single insert games'
        return s





def multi_row_games(file_name='games.csv'):
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    df = pd.read_csv(file_name, sep=',', header=None)
    cols = str(df.columns.values.tolist()).replace('[','').replace(']', '')
    values = list(map(tuple, df.values))
    half = len(values) // 2
    valuesA = values[:half]
    valuesB = values[half:]
    start = time.time()
    
    sql = "INSERT INTO games (GameID, Date, Stadium, Result, Attendance, TicketRevenue) VALUES (%s, %s, %s, %s, %s, %s);"
    try:
        cursor.executemany(sql, valuesA)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    try:
        cursor.executemany(sql, valuesB)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    db.close()
    end = time.time()
    print(end - start, 'seconds for multi row games')
    s=str(end - start) + 'seconds'
    return s





def bulk_games(file_name='games.csv'):
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password',
                        allow_local_infile=True)
    cursor = db.cursor()
    start = time.time()
    sql = "LOAD DATA LOCAL INFILE \'%s\' INTO TABLE games FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' (GameID, Date, Stadium, Result, Attendance, TicketRevenue)" % (file_name)
    try:
        cursor.execute(sql)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    db.close()
    end = time.time()
    print(end - start, 'seconds for bulk games')
    s=str(end - start) + 'seconds'
    return s





def insert_play(file_name='plays.csv'):
    vals=[]
    with open(file_name) as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            vals.append(tuple(row))
        db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
        cursor = db.cursor()
        start = time.time()
        for row in vals:
            sql = "INSERT INTO Play (PlayerID, GameID) VALUES (%s, %s)"
            try:
                cursor.execute(sql, row)
            except sqlcon.Error as e:
                return e.msg
        db.commit()
        db.close()
        end = time.time()
        print(end - start, 'seconds for single insert play')
        s=str(end - start) + 'seconds'
        return s





def multi_row_play(file_name='play.csv'):
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    df = pd.read_csv(file_name, sep=',', header=None)
    cols = str(df.columns.values.tolist()).replace('[','').replace(']', '')
    values = list(zip(df[0], df[1]))
    half = len(values) // 2
    valuesA = values[:half]
    valuesB = values[half:]
    start = time.time()
    
    sql = "INSERT INTO Play (PlayerID, GameID) VALUES (%s, %s)"
    try:
        cursor.executemany(sql, valuesA)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    try:
        cursor.executemany(sql, valuesB)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    db.close()
    end = time.time()
    print(end - start, 'seconds for multi row play')
    s=str(end - start) + 'seconds'
    return s





def bulk_play(file_name='play.csv'):
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password',
                        allow_local_infile=True)
    cursor = db.cursor()
    start = time.time()
    sql = "LOAD DATA LOCAL INFILE \'%s\' INTO TABLE play FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' (PlayerID, GameID)" % (file_name)
    try:
        cursor.execute(sql)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    db.close()
    end = time.time()
    print(end - start, 'seconds for bulk play')
    s=str(end - start) + 'seconds'
    return s





# Generate Data Functions

def generate_teams_data(numberOfRows, output_filename='teams.csv'):
    """creates a csv file with numberOfRows number of rows for the teams table
    teams Table:
    TeamID     int,
    Team_name    varchar(20) not null,
    city         varchar(20) not null,
    primary key (TeamID)
    """
    
    ID_set=set() # storing the ID's since primary key should be unique.
    list_of_rows=[]
    with open(output_filename, mode='w', newline='') as player_file:
        writer = csv.writer(player_file, delimiter=',')
        
        for _ in range(numberOfRows):
            TeamID = randint(1, 1000000000)
            while(TeamID in ID_set):
                TeamID = randint(1, 1000000000)
            ID_set.add(TeamID)
            Team_name = random.choice(NFL_teams)
            city = random.choice(NFL_city)
            
            list_of_rows.append([TeamID, Team_name, city])
        writer.writerows(list_of_rows)
    print("%s generated" % (output_filename))





def generate_players_data(numberOfRows, output_filename='players.csv'):
    """creates a csv file with numberOfRows number of rows for the player table
    Player Table: 
    PlayerID     int,
    TeamID     int,
    FirstName:   varchar(20) not null,
    LastName:    varchar(20) not null,
    Position     varchar(2)  check (Position in ('QB','RB','WR')),
    Touchdowns   int check   (Touchdowns > 0),
    Total_Yards  int check   (Total_Yards > 0),
    Salary       int check   (Salary > 0 ),
    primary key (PlayerID)
    foreign key (TeamID) references teams (TeamID)
    """
    
    ID_set=set() # storing the ID's since primary key should be unique.
    list_of_rows=[]
    with open(output_filename, mode='w', newline='') as player_file:
        writer = csv.writer(player_file, delimiter=',')
        
        Team_IDs = get_TeamIDs()
        
        for _ in range(numberOfRows):
            PlayerID = randint(1, 1000000000)
            TeamID = random.choice(Team_IDs)
            while(PlayerID in ID_set):
                PlayerID = randint(1, 1000000000)
                TeamID = random.choice(Team_IDs)
            ID_set.add(PlayerID)
            FirstName = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(5, 10))
            LastName = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(5, 10))
            Team_name = random.choice(NFL_teams)
            Position = random.choice(('QB','RB','WR'))
            Touchdowns = randint(1,1000)
            Total_Yards = randint(1, 1000000)
            Salary = randint(1000, 1000000)
            
            list_of_rows.append([PlayerID, TeamID, FirstName, LastName, Position, Touchdowns, Total_Yards, Salary])
        writer.writerows(list_of_rows)
    print("%s generated" % (output_filename))





def generate_games_data(numberOfRows, output_filename='games.csv'):
    """creates a csv file with numberOfRows number of rows for the game table
    Game Table:
    GameID         int,
    Date           date,
    Stadium        varchar(20),
    Result         char check (Result in ('W','L','T')),
    Attendance     int check (Attendance > 0),
    Ticket_Revenue int check (Ticket_Revenue > 0),
    primary key (GameID)
    """
    ID_set=set()
    list_of_rows=[]
    with open(output_filename, mode='w', newline='') as game_file:
        writer = csv.writer(game_file, delimiter=',')
        
        for _ in range(numberOfRows):
            GameID = randint(1, 1000000)
            while(GameID in ID_set):
                GameID = randint(1, 1000000)
            ID_set.add(GameID)
            Date = random_date()
            Stadium = random.choice(NFL_stadiums)
            Result = random.choice(('W','L','T'))
            Attendance = randint(1, 1000000)
            Ticket_Revenue = randint(1, 1000000000)
            
            list_of_rows.append([GameID, Date, Stadium, Result, Attendance, Ticket_Revenue])
        writer.writerows(list_of_rows)
    print("%s generated" % (output_filename))





def generate_play_data(numberOfRows, output_filename='play.csv'):
    """creates a csv file with numberOfRows number of rows for the play table
    Play Table:
    PlayerID       int,
    GameID         int,
    primary key (PlayerID, GameID),
    foreign key (PlayerID) references Players(PlayerID),
    foreign key (GameID) references Games(GameID),
    """
    ID_set=set()
    list_of_rows=[]
    with open(output_filename, mode='w', newline='') as play_file:
        writer = csv.writer(play_file, delimiter=',')
        
        player_IDs = get_PlayerIDs()
        game_IDs = get_GameIDs()
        
        for _ in range(numberOfRows):
            PlayerID = random.choice(player_IDs)
            GameID = random.choice(game_IDs)
            while((PlayerID, GameID) in ID_set):
                PlayerID = random.choice(player_IDs)
                GameID = random.choice(game_IDs)
            ID_set.add((PlayerID, GameID))
            
            list_of_rows.append([PlayerID, GameID])
        writer.writerows(list_of_rows)
    print("%s generated" % (output_filename))





def retrieve_teams():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    sql = "SELECT * FROM TEAMS"
    cursor.execute(sql)
    
    with user_out:
        print("Team ID, Team Name, City")
        for a in cursor:
            print(a)
    db.close()

def retrieve_players():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    sql = "SELECT * FROM PLAYERS"
    cursor.execute(sql)
    
    with user_out:
        print("Player ID, Team ID, First Name, Last Name, Position, Touchdowns, Total Yards, Salary")
        for a in cursor:
            print(a)
    db.close()

def retrieve_games():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    sql = "SELECT * FROM GAMES"
    cursor.execute(sql)
    
    with user_out:
        print("Game ID, Date, Stadium, Result, Attendance, Ticket Revenue")
        for a in cursor:
            print(a)
    db.close()
            
def retrieve_play():
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    sql = "SELECT * FROM PLAY"
    cursor.execute(sql)
    
    with user_out:
        print("Player ID, Game ID")
        for a in cursor:
            print(a)
    db.close()

def retrieve_average(table, column):
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='Project', 
                        auth_plugin='mysql_native_password')
    cursor = db.cursor()
    sql = "SELECT AVG(" + column + ") FROM " + table
    cursor.execute(sql)
    
    with user_out:
        print("Average of ", table, " and ", " column: ")
        for a in cursor:
            print(a)
    db.close()





user_out = w.Output(layout={'border': '1px solid black'})

# Tables (remove, retrieve, average)
table = w.Text(description="Table: ")
table_type = w.RadioButtons(
    options=['play', 'games','players', 'teams']
)

# File Upload
file_upload = w.FileUpload(
    accept='',  # Accepted file extension e.g. '.txt', '.pdf', 'image/*', 'image/*,.pdf'
    multiple=False  # True to accept multiple files upload else False
)

# Single insert
single_insert = w.Button(description="Single Insert")





def _on_single_insert_clicked(single_insert):
    user_out.clear_output()
    with user_out:
        if table_type.value == 'teams':
            insert_teams(file_name=next(iter(file_upload.value)))    
        elif table_type.value == 'players':
            insert_players(file_name=next(iter(file_upload.value)))
        elif table_type.value == 'games':
            insert_games(file_name=next(iter(file_upload.value)))
        elif table_type.value == 'play':
            insert_play(file_name=next(iter(file_upload.value)))
single_insert.on_click(_on_single_insert_clicked)


# Multiple row insert
multiple_row_insert = w.Button(description="Multiple Row Insert")





def _on_multiple_row_insert_clicked(multiple_row_insert):
    user_out.clear_output()
    with user_out:
        if table_type.value == 'teams':
            multi_row_teams(file_name=next(iter(file_upload.value)))    
        elif table_type.value == 'players':
            multi_row_players(file_name=next(iter(file_upload.value)))
        elif table_type.value == 'games':
            multi_row_games(file_name=next(iter(file_upload.value)))
        elif table_type.value == 'play':
            multi_row_play(file_name=next(iter(file_upload.value)))
multiple_row_insert.on_click(_on_multiple_row_insert_clicked)





# Load data syntax
load_data_syntax = w.Button(description="Load Data Syntax")

def _on_load_data_syntax_clicked(load_data_syntax):
    user_out.clear_output()
    with user_out:
        if table_type.value == 'teams':
            bulk_teams(file_name=next(iter(file_upload.value)))    
        elif table_type.value == 'players':
            bulk_players(file_name=next(iter(file_upload.value)))
        elif table_type.value == 'games':
            bulk_games(file_name=next(iter(file_upload.value)))
        elif table_type.value == 'play':
            bulk_play(file_name=next(iter(file_upload.value)))
load_data_syntax.on_click(_on_load_data_syntax_clicked)





# Remove a table
remove_table = w.Button(description="Remove Table")

def _on_remove_table_clicked(remove_table):
    user_out.clear_output()
    with user_out:
        if table_type.value == 'teams':
            print("Teams ")
            delete_teams()    
        elif table_type.value == 'players':
            print("Players ")
            delete_players()
        elif table_type.value == 'games':
            print("Games ")
            delete_games()
        elif table_type.value == 'play':
            print("Play ")
            delete_play()
remove_table.on_click(_on_remove_table_clicked)





# Retrieve a table
retrieve_table = w.Button(description="Retrieve Table")

def _on_retrieve_table_clicked(retrieve_table):
    user_out.clear_output()
    with user_out:
        if table_type.value == 'teams':
            retrieve_teams()    
        elif table_type.value == 'players':
            retrieve_players()
        elif table_type.value == 'games':
            retrieve_games()
        elif table_type.value == 'play':
            retrieve_play()
retrieve_table.on_click(_on_retrieve_table_clicked)





# Get Average

column = w.Text(description ="Column: ")
average_table_column = w.Button(description="Find Average")

def _on_average_table_column_clicked(average_table_column):
    user_out.clear_output()
    with user_out:
        retrieve_average(table_type.value, column.value)
        
average_table_column.on_click(_on_average_table_column_clicked)

upload = w.VBox([
    w.Label(value="Select a text file:"),
    file_upload,
    w.Label(value="Select table type: "),
    table_type,
    w.Label(value="Use one of the following methods to upload data:"),
    single_insert,
    multiple_row_insert,
    load_data_syntax,
    user_out,
])

delete = w.VBox([
    w.Label(value="Select table type: "),
    table_type, 
    remove_table,
    user_out,
])

retrieve = w.VBox([
    w.Label(value="Select table type: "),
    table_type, 
    retrieve_table,   
    user_out
])

average = w.VBox([
    w.Label(value="Select table type: "),
    table_type, 
    column,
    average_table_column,
    user_out,
])

user_interface = w.Accordion(children=[upload, delete, retrieve, average])
user_interface.set_title(0, 'upload')
user_interface.set_title(1, 'delete')
user_interface.set_title(2, 'retrieve')
user_interface.set_title(3, 'average')





# clear tables

delete_play()
delete_games()
delete_players()
delete_teams()


# test functions 1000

generate_teams_data(1000, "teams1000.csv")
insert_teams("teams1000.csv")

generate_players_data(1000, "players1000.csv")
insert_players("players1000.csv")

generate_games_data(1000, "games1000.csv")
insert_games("games1000.csv")

generate_play_data(1000, "play1000.csv")
insert_play("play1000.csv")





delete_play()
delete_games()
delete_players()
delete_teams()
multi_row_teams("teams1000.csv")
multi_row_players("players1000.csv")
multi_row_games("games1000.csv")
multi_row_play("play1000.csv")





delete_play()
delete_games()
delete_players()
delete_teams()


bulk_teams("teams1000.csv")
bulk_players("players1000.csv")
bulk_games("games1000.csv")
bulk_play("play1000.csv")





# clear tables

delete_play()
delete_games()
delete_players()
delete_teams()


# test functions 100000

generate_teams_data(100000, "teams100000.csv")
insert_teams("teams100000.csv")

generate_players_data(100000, "players100000.csv")
insert_players("players100000.csv")

generate_games_data(100000, "games100000.csv")
insert_games("games100000.csv")

generate_play_data(100000, "play100000.csv")
insert_play("play100000.csv")





delete_play()
delete_games()
delete_players()
delete_teams()
multi_row_teams("teams100000.csv")
multi_row_players("players100000.csv")
multi_row_games("games100000.csv")
multi_row_play("play100000.csv")





delete_play()
delete_games()
delete_players()
delete_teams()


bulk_teams("teams100000.csv")
bulk_players("players100000.csv")
bulk_games("games100000.csv")
bulk_play("play100000.csv")





# clear tables

delete_play()
delete_games()
delete_players()
delete_teams()


# test functions 1000000

generate_teams_data(1000000, "teams1000000.csv")
insert_teams("teams1000000.csv")

generate_players_data(1000000, "players1000000.csv")
insert_players("players1000000.csv")

generate_games_data(1000000, "games1000000.csv")
insert_games("games1000000.csv")

generate_play_data(1000000, "play1000000.csv")
insert_play("play1000000.csv")





delete_play()
delete_games()
delete_players()
delete_teams()
multi_row_teams("teams1000000.csv")
multi_row_players("players1000000.csv")
multi_row_games("games1000000.csv")
multi_row_play("play1000000.csv")





delete_play()
delete_games()
delete_players()
delete_teams()
bulk_teams("teams1000000.csv")
bulk_players("players1000000.csv")
bulk_games("games1000000.csv")
bulk_play("play1000000.csv")





delete_play()
delete_games()
delete_players()
delete_teams()





dataSize = [1000, 100000, 1000000]
teamsTimeSingle = [0.13682198524475098, 12.603698968887329, 126.55780601501465]
teamsTimeMulti = [0.012310028076171875, 1.530973196029663, 13.74002981185913]
teamsTimeBulk = [0.007586240768432617, 0.761115312576294,14.739612817764282]

playersTimeSingle = [0.18940401077270508, 16.467517852783203, 197.9443929195404]
playersTimeMulti = [0.02847886085510254, 3.152730703353882, 64.35697484016418]
playersTimeBulk = [0.01456904411315918, 2.0501699447631836, 48.638970136642456]

gamesTimeSingle = [0.14987802505493164, 14.354168176651001, 142.04881381988525]
gamesTimeMulti = [0.017287254333496094, 1.7814829349517822, 19.723872900009155]
gamesTimeBulk = [0.008643150329589844, 0.85105299949646, 9.453365087509155]

playTimeSingle = [0.13208985328674316, 14.47389817237854, 164.2709460258484]
playTimeMulti = [0.0174257755279541, 2.274960994720459, 43.21596598625183]
playTimeBulk = [0.014802932739257812, 1.9068458080291748, 35.553751707077026]





df2=pd.DataFrame({'x': dataSize, 'Single': teamsTimeSingle, 'Multi': teamsTimeMulti, 'Bulk': teamsTimeBulk })
 
# multiple line plot
plt.plot( 'x', 'Single', data=df2, marker='o', color='skyblue', linewidth=2)
plt.plot( 'x', 'Multi', data=df2, marker='x', color='yellow', linewidth=4)
plt.plot( 'x', 'Bulk', data=df2, marker='p', color='red', linewidth=2, linestyle='dashed')
plt.title('Time to Insert Teams')
plt.xlabel('Number of Entries')
plt.ylabel('Time (seconds)')
plt.legend()
plt.show()





df3=pd.DataFrame({'x': dataSize, 'Single': playersTimeSingle, 'Multi': playersTimeMulti, 'Bulk': playersTimeBulk })
 
# multiple line plot
plt.plot( 'x', 'Single', data=df3, marker='o', color='skyblue', linewidth=2)
plt.plot( 'x', 'Multi', data=df3, marker='x', color='yellow', linewidth=4)
plt.plot( 'x', 'Bulk', data=df3, marker='p', color='red', linewidth=2, linestyle='dashed')
plt.title('Time to Insert Players')
plt.xlabel('Number of Entries')
plt.ylabel('Time (seconds)')
plt.legend()
plt.show()




df4=pd.DataFrame({'x': dataSize, 'Single': gamesTimeSingle, 'Multi': gamesTimeMulti, 'Bulk': gamesTimeBulk })
 
# multiple line plot
plt.plot( 'x', 'Single', data=df4, marker='o', color='skyblue', linewidth=2)
plt.plot( 'x', 'Multi', data=df4, marker='x', color='yellow', linewidth=4)
plt.plot( 'x', 'Bulk', data=df4, marker='p', color='red', linewidth=2, linestyle='dashed')
plt.title('Time to Insert Games')
plt.xlabel('Number of Entries')
plt.ylabel('Time (seconds)')
plt.legend()
plt.show()




df5=pd.DataFrame({'x': dataSize, 'Single': playTimeSingle, 'Multi': playTimeMulti, 'Bulk': playTimeBulk })
 
# multiple line plot
plt.plot( 'x', 'Single', data=df5, marker='o', color='skyblue', linewidth=2)
plt.plot( 'x', 'Multi', data=df5, marker='x', color='yellow', linewidth=4)
plt.plot( 'x', 'Bulk', data=df5, marker='p', color='red', linewidth=2, linestyle='dashed')
plt.title('Time to Insert Play')
plt.xlabel('Number of Entries')
plt.ylabel('Time (seconds)')
plt.legend()
plt.show()




display(user_interface)
user_out.clear_output()