from dbconn import connect_to_db
from base_scrapper import *
from operations import *
from create_database_table import *
from generate_files import *
from insert_update import upsert_insert_data

def main():

    # hard coded seasons
    seasons = ['2006-07','2007-08','2008-09','2009-10','2010-11','2011-12',
            '2012-13','2013-14','2014-15','2015-16','2016-17','2017-18','2018-19',
            '2019-20','2020-21','2021-22', '2022-23', '2023-24']
    
    # get player ids to be used below
    player_ids = get_player_ids()

    # get all data
    # gameweeks = get_gameweeks()
    # players = get_player_stat()
    # teams = get_team_stat()
    # positions = get_positions()
    # fixtures = get_fixtures(player_ids)
    # history = get_history(player_ids)
    # history_past = get_history_past(player_ids)

    # tables and their respective data
    # tables_data = {
    #         'gameweeks': gameweeks,
    #         'players': players,
    #         'teams': teams,
    #         'positions': positions,
    #         'fixtures': fixtures,
    #         'history': history,
    #         'history_past': history_past
    #     }
    
    # connect to the database
    conn, cursor = connect_to_db()
    
    # create tables in the database
    # create_table(tables_data, cursor)
    

    # update or insert data into tables
    # upsert_insert_data(tables_data, cursor)

    """
    uncomment to generate csv files if needed
    update sql query in generate_files.py as you seem fit
    """
    # save season data as csv files (optional)
    for season in seasons:
        fetch_and_save_season_data(season, cursor)

    # generate csv file data for ongoing season
    # player_current_season_data(cursor)

    # generate data for past seasons. Data only availabe from 2017-18 season
    # for season in seasons:
    #     player_season_data(season, cursor)

    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()