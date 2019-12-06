/* 
	Table: Teams
	Assumptions: None
*/
create table teams
(
	TeamID		int,
    TeamName	varchar(255),
    City		varchar(255),
    primary key (TeamID)
);

/*	
	Table: Players
	Assumptions: Player FirstName and LastName cannot be NULL.
				 Team name CAN be NULL (for free-agents).
                 Touchdowns, TotalYards, and Salary each must be no less than 0.
*/
create table players
(
	PlayerID	int,
    TeamID		int,
    FirstName	varchar(255) NOT NULL,
    LastName	varchar(255) NOT NULL,
    Position	varchar(2),
    CONSTRAINT chk_Position CHECK (Position IN ('QB', 'RB', 'WR')),
    Touchdowns	int check (Touchdowns >= 0),
    TotalYards	int check (TotalYards >= 0),
    Salary		int check (Salary >= 0),
    primary key (PlayerID),
    foreign key (TeamID) references teams (TeamID)
);
 
  /*
	Table: Games
    Assumptions:	All games in the table have been played already (no future games).
					Neither game date, game stadium, nor game result can be NULL. 
                    Both attendance and TicketRevenue must be no less than 0.
 */
create table games
(
	GameID		int,
    Date		DATE NOT NULL,
    Stadium		varchar(255) NOT NULL,
    Result		varchar(1) NOT NULL,
    CONSTRAINT chk_Result CHECK (Result IN ('W', 'L', 'T')),
    Attendance	int check (Attendance >= 0),
    TicketRevenue int check (TicketRevenue >= 0),
    primary key (GameID)
);
  
/*
	Table: Play
	Assumptions: None
*/
create table play
(
	PlayerID	int,
    GameID		int,
    primary key (PlayerID, GameID), 
    foreign key (PlayerID) references players (PlayerID),
    foreign key (GameID) references games (GameID)
);
