![Fantasy Premier League Logo](image/FFL-23-24.png)

# FPL Oracle: Unleashing Fantasy Premier League Data

## Introduction

This project emerged from a passion for data analysis and the popular Fantasy Premier League (FPL) game. It stands on the shoulders of giants, drawing inspiration from the excellent work done by the team at https://github.com/vaastav/Fantasy-Premier-League. Their tireless efforts in curating FPL data across multiple seasons have laid the groundwork for projects like this one.

## Project Objective

This project is designed to extract, transform, and load data from the Fantasy Premier League API into a local / cloud Oracle database. The project aims to automate data retrieval, processing, and storage for player and team statistics, enabling in-depth analysis of FPL data.

### Key Features

- **Data Extraction**: Fetches data from the FPL API, including player stats, team details, and historical information.
- **Data Transformation**: Cleans, processes, and formats the data for consistency and analysis.
- **Data Loading**: Dynamically create database tables and Stores the processed data in an Oracle database.
- **Generate Data Files**: Supports generating data files how you see fit using SQL query.
- **Scheduling and Automation**: Schedule ETL run using Prefect

## Getting Started

### Prerequisites

Before setting up the project, ensure you have the following software installed:

- **Python 3.8+**
- **Oracle Database** 
- **oracledb** for connecting Python with Oracle
- **Prefect** for scheduling and automation
- **pandas**, **requests**, **tqdm**, Python libraries



### Installation Instructions

1. The project was all completed using VSCode.

To install the required Python libraries, run:
```
bash
pip install oracledb pandas requests tqdm prefect
```

```
windows
pip install oracledb pandas requests tqdm prefect
```
2. Clone the repository
```
  git clone https://github.com/bonsuot/fantasy-premier-league
  cd fantasy-premier-league
```
3. Database Setup
   - Ensure your Oracle database is up and running. You can install the Oracle db extension in VSCode. Refer to dbconn.py to setup your database
   - Download the Oracle Instant Client for your operating system from [here](https://www.oracle.com/database/technologies/instant-client.html)
   - See [instructions](https://docs.oracle.com/en/database/oracle/developer-tools-for-vscode/getting-started/gettingstarted.html) for how to set up your database connection
   - Update the database connection details in the dbconn.py file to reflect your Oracle connection settings.
  
4. Learn more about using prefect for scheduling and automation. Visit [Prefect Quickstart](https://docs.prefect.io/3.0/get-started/quickstart)

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

For running prefect ETL schedule locally
```
bash
$ python3 fpl_etl.py
```

```
windows
> python fpt_etl.py
```

### Python Files
- **dbconn.py** : For oracle database connection
- **base_scrapper.py** : Fectching data from Fantasy API
- **operations.py** : Generating pandas dataframes to load into the database
- **create_database_table.py** : Dynamically create tables base on pandas dataframes 
- **insert_update.py** : Insert data into tables or Update table data when necessary
- **generate_files.py** : Generating csv data files
- **fpl_etl.py** : Prefect flow script
- **deployment.py** : ETL automation script

### Expected Output

- All data is stored in the specified Oracle database tables on your local machine / Oracle cloud
- data/players: contains all players stats in past seasons
- data/teams: contains teams in past / current sesasons
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

Reach out with any questions and contributions

Do not forget to reference this repo if used in any projects

## Contact
- Author: Tutu Bonsu (kt.bonsu@gmail.com)
- GitHub: https://github.com/bonsuot
