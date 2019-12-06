#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
import pandas as pd
import string
import random
import csv
from random import randint
import time


# In[2]:


NFL_teams=('Arizona Cardinals', 'Atlanta Falcons', 'Baltimore Ravens', 'Buffalo Bills', 'Carolina Panthers', 'Chicago Bears',
'Cincinnati Bengals', 'Cleveland Browns', 'Dallas Cowboys', 'Denver Broncos', 'Detroit Lions', 'Green Bay Packers',
'Houston Texans','Indianapolis Colts', 'Jacksonville Jaguars', 'Kansas City Chiefs', 'Miami Dolphins', 'Minnesota Vikings',
'New England Patriots', 'New Orleans Saints', 'New York Giants', 'New York Jets', 'Oakland Raiders', 'Philadelphia Eagles',
'Pittsburgh Steelers', 'St. Louis Rams', 'San Diego Chargers', 'San Francisco 49ers', 'Seattle Seahawks','Tampa Bay Buccaneers',
'Tennessee Titans','Washington Redskins')


# In[3]:


NFL_stadiums=('Arrowhead', 'AT&T', 'Bank of America', 'Broncos', 'CenturyLink Field', 'FedExField', 'FirstEnergy',
             'Ford Field', 'Gillette', 'Hard Rock', 'Heinz Field', 'Lambeau Field')


# In[4]:


NFL_city=('Arizona', 'Atlanta', 'Baltimore', 'Buffalo Bills', 'Carolina', 'Chicago',
'Cincinnati', 'Cleveland', 'Dallas', 'Denver', 'Detroit','Houston','Indianapolis',
'Jacksonville', 'Kansas', 'Miami', 'Minnesota','New England Patriots', 'New Orleans Saints',
'New York Giants', 'New York Jets', 'Oakland Raiders', 'Philadelphia Eagles',
'Pittsburgh','San Diego', 'San Francisco', 'Seattle','Tampa Bay','Tennessee','Washington')


# In[61]:


def random_date():
    year = randint(2000, 2019)
    month = randint(1, 12)
    if(month<10):
        month='0'+str(month)
    day = randint(1, 30)
    if(day<10):
        day='0'+str(day)
    return str(year)+str(month)+str(day)


# In[6]:


def get_TeamIDs():
    database_conector = mysql.connector.connect(user='root', password='12345',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
    pointer = database_conector.cursor()
    pointer.execute("SELECT TeamID FROM teams")
    result = pointer.fetchall()
    return [item[0] for item in result]


# In[7]:


def get_PlayerIDs():
    db = mysql.connector.connect(user='root', password='12345',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
    cursor = db.cursor()
    cursor.execute("SELECT PlayerID FROM Players")
    result = cursor.fetchall()
    return [item[0] for item in result]


# In[8]:


def get_GameIDs():
    db = mysql.connector.connect(user='root', password='12345',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
    cursor = db.cursor()
    cursor.execute("SELECT GameID FROM Games")
    result = cursor.fetchall()
    #return result
    return [item[0] for item in result]


# In[9]:


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


# In[10]:


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


# In[11]:


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


# In[12]:


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


# In[29]:


def delete_players():
    database_conector = mysql.connector.connect(user='root', password='usa220231609',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
    pointer = database_conector.cursor()
    sql = "DELETE FROM players"
    pointer.execute(sql)
    database_conector.commit()


# In[14]:


def delete_teams():
    database_conector = mysql.connector.connect(user='root', password='usa220231609',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
    pointer = database_conector.cursor()
    sql = "DELETE FROM teams"
    pointer.execute(sql)
    database_conector.commit()


# In[15]:


def delete_games():
    database_conector = mysql.connector.connect(user='root', password='usa220231609',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
    pointer = database_conector.cursor()
    sql = "DELETE FROM Games"
    pointer.execute(sql)
    database_conector.commit()


# In[16]:


def delete_play():
    database_conector = mysql.connector.connect(user='root', password='usa220231609',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
    pointer = database_conector.cursor()
    sql = "DELETE FROM Play"
    pointer.execute(sql)
    database_conector.commit()


# In[23]:


def insert_teams(file_name='teams.csv'):
    vals=[]
    with open(file_name) as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            vals.append(tuple(row))
        database_conector = mysql.connector.connect(user='root', password='usa220231609',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
        pointer = database_conector.cursor()
        start = time.time()
        for row in vals:
            sql = "INSERT INTO teams (TeamID, TeamName, city) VALUES (%s, %s, %s)"
            try:
                pointer.execute(sql, row)
            except mysql.connector.Error as e:
                return e.msg
        database_conector.commit()
        end = time.time()
        print(end - start, 'seconds')
        s=str(end - start) + 'seconds'
        return s


# In[49]:


def insert_players(file_name='players.csv'):
    vals=[]
    with open(file_name) as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            vals.append(tuple(row))
        database_conector = mysql.connector.connect(user='root', password='usa220231609',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
        pointer = database_conector.cursor()
        start = time.time()
        for row in vals:
            sql = "INSERT INTO players (PlayerID, TeamID, FirstName, LastName, Position, Touchdowns, TotalYards, Salary) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            try:
                pointer.execute(sql, row)
            except mysql.connector.Error as e:
                return e.msg
        database_conector.commit()
        end = time.time()
        print(end - start, 'seconds')
        s=str(end - start) + 'seconds'
        return s


# In[53]:


def insert_games(file_name='games.csv'):
    vals=[]
    with open(file_name) as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            vals.append(tuple(row))
        database_conector = mysql.connector.connect(user='root', password='usa220231609',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
        pointer = database_conector.cursor()
        start = time.time()
        for row in vals:
            sql = "INSERT INTO Games (GameID, Date, Stadium, Result, Attendance, TicketRevenue) VALUES (%s, %s, %s, %s, %s, %s)"
            try:
                pointer.execute(sql, row)
            except mysql.connector.Error as e:
                return e.msg
        database_conector.commit()
        end = time.time()
        print(end - start, 'seconds')
        
        s=str(end - start) + 'seconds'
        return s


# In[20]:


def insert_play(file_name='plays.csv'):
    vals=[]
    with open(file_name) as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            vals.append(tuple(row))
        database_conector = mysql.connector.connect(user='root', password='usa220231609',
                                 host='127.0.0.1', database='nfl',
                                 auth_plugin='mysql_native_password')
        pointer = database_conector.cursor()
        start = time.time()
        for row in vals:
            sql = "INSERT INTO Play (PlayerID, GameID) VALUES (%s, %s)"
            try:
                pointer.execute(sql, row)
            except mysql.connector.Error as e:
                return e.msg
        database_conector.commit()
        end = time.time()
        print(end - start, 'seconds')
        s=str(end - start) + 'seconds'
        return s


# In[79]:


delete_play()


# In[80]:


delete_games()


# In[81]:


delete_players()


# In[86]:


delete_teams()


# In[87]:


generate_teams_data(1000,'teams1000.csv')


# In[88]:


insert_teams('teams1000.csv')


# In[41]:


generate_players_data(1000,'players1000.csv')


# In[50]:


insert_players('players1000.csv')


# In[62]:


generate_games_data(1000, 'games1000.csv')


# In[63]:


insert_games('games1000.csv')


# In[66]:


generate_play_data(1000,'play1000.csv')


# In[67]:


insert_play('play1000.csv')

