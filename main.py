from dbconn import *
from base_scrapper import *
from operations import *
from create_insert import *
from generate_files import *
from update_table import execute_plsql_update

def main():

    # hard coded seasons
    seasons = ['2006-07','2007-08','2008-09','2009-10','2010-11','2011-12',
            '2012-13','2013-14','2014-15','2015-16','2016-17','2017-18','2018-19',
            '2019-20','2020-21','2021-22', '2022-23', '2023-24']
    
    
    # get all data
    gameweeks = get_gameweeks()
    players = get_player_stat()
    teams = get_team_stat()
    positions = get_positions()

    # get player ids to be used below
    player_ids = players['player_id'].to_list()

    fixtures = get_fixtures(player_ids)
    history = get_history(player_ids)
    history_past = get_history_past(player_ids)

    # tables and their respective data
    tables_data = {
            'gameweeks': gameweeks,
            'players': players,
            'teams': teams,
            'positions': positions
        }
    
    # these tables will always be dropped first and recreated for any reruns
    tables_data_non_pk = {
            'fixtures': fixtures,
            'history': history,
            'history_past': history_past
        }

    # connect to the database
    conn = connect_to_db()
    cursor = conn.cursor()

    """
    create tables in the database
    """
    create_table(tables_data, cursor)
    create_table_non_pk(tables_data_non_pk, cursor)

    """
    Insert data. Can comment below out after
    first insertion of data. Update process will update tables
    when you run this again
    """
    insert_data(tables_data, cursor)
    insert_data(tables_data_non_pk, cursor)

    """
    Uncomment to update tables when re-running this script
    Make sure to comment out insert_data(tables_data, cursor) above
    """
    # for table_name, df in tables_data.items():
    #     execute_plsql_update(df, cursor, table_name)


    # save season data as csv files (optional)
    for season in seasons:
        fetch_and_save_season_data(season, cursor)

    # generate csv file data for ongoing season
    player_current_season_data(cursor)

    # generate data for past seasons. Data only availabe from 2017-18 season
    for season in seasons:
        player_season_data(season, cursor)

    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()