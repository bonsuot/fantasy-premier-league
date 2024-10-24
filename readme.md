![Fantasy Premier League Logo](image/FFL-23-24.png)

# FPL Oracle: Unleashing Fantasy Premier League Data

## Introduction

This project emerged from a passion for data analysis and the popular Fantasy Premier League (FPL) game. It stands on the shoulders of giants, drawing inspiration from the excellent work done by the team at https://github.com/vaastav/Fantasy-Premier-League. Their tireless efforts in curating FPL data across multiple seasons have laid the groundwork for projects like this one.

## Project Objective

This project is designed to extract, transform, and load data from the Fantasy Premier League API into a local Oracle database. The project aims to automate data retrieval, processing, and storage for player and team statistics, enabling in-depth analysis of FPL data.

### Key Features

- **Data Extraction**: Fetches data from the FPL API, including player stats, team details, and historical information.
- **Data Transformation**: Cleans, processes, and formats the data for consistency and analysis.
- **Data Loading**: Dynamically create database tables and Stores the processed data in an Oracle database.
- **Generate Data Files**: Supports generating data files how you see fit using SQL query.
- **Scheduling and Automation**: In progress.

## Getting Started

### Prerequisites

Before setting up the project, ensure you have the following software installed:

- **Python 3.8+**
- **Oracle Database** 
- **oracledb** for connecting Python with Oracle
- **pandas**, **requests**, **tqdm**, Python libraries

The project was all completed using VSCode.

To install the required Python libraries, run:
```
bash
pip install oracledb pandas requests tqdm 
```

```
windows
pip install oracledb pandas requests tqdm 
```

### Installation Instructions

1. Clone the repository
```
  git clone https://github.com/bonsuot/fantasy-premier-league
  cd fantasy-premier-league
```
2. Database Setup
   - Ensure your Oracle database is up and running. You can install the Oracle db extension in VSCode. Refer to dbconn.py to setup your database
   - Download the Oracle Instant Client for your operating system from https://www.oracle.com/database/technologies/instant-client.html
   - Update the database connection details in the dbconn.py file to reflect your Oracle instance settings.

### How to run the project

Execute main.py by running in interactive window in VSCode or on the command line 
```
bash
$ python3 main.py
```

```
windows
> python main.py
```

### Python Files
- **dbconn.py** : For creating and connecting to your Oracle database. Make sure to input your username and password
- **base_scrapper.py** : Fectching data from FPL API
- **operations.py** : Generating pandas dataframes to load into the database
- **create_database_table.py** : Dynamically create tables base on pandas dataframes **insert_update.py** : Insert data into tables or Update table data when necessary
- **generate_files.py** : Generating csv data files. You can follow the example to query your database to generate any data file you want

### Expected Output

- All data is stored in the specified Oracle database tables on your local machine. Few csv files have been generated.
- data/players: contains all players stats in past seasons
- data/teams: contains teams in past sesasons
- data/current_season_stats.csv contains all player stats in ongoing season

## Tables
- **PLAYERS**: Basic information on all Premier League players
- **TEAMS**: Basic information on all 20 PL teams
- **POSITIONS**: The different FPL postions (FWD, MID, DEF, GKP)
- **GAMEWEEKS**: Gameweek specific stats for the particular season
- **FIXTURES**:Fixtures for the current season
- **HISTORY**: In-depth info on a specific player in the current season
- **HISTORY_PAST**: In-depth info on a specific player in the past seasons


## Troubleshooting and FAQs

Common Issues

- Database Connection Errors: Ensure the Oracle client libraries are installed and configured correctly.
  
- Missing Modules: Run pip install -r requirements.txt to install dependencies.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact
- Author: Osei Bonsu (kt.bonsu@gmail.com)
- GitHub: https://github.com/bonsuot