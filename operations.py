import pandas as pd
import hashlib
import requests
from tqdm import tqdm
from base_scrapper import *


def calculate_hash(row, columns):
    """
    function to calculate hash values to add as an additional column
    the goal is for detecting changes between source and target data
    to make row base updates
    """
    row_str = ''.join(str(row[col]) for col in columns)
    return hashlib.md5(row_str.encode()).hexdigest()

with requests.Session() as session:
# get gameweek data
    def get_gameweeks():

        df = get_data("events", session) # Contains gameweek stats

        df = df[['id','name','deadline_time','deadline_time_epoch','average_entry_score','finished',
                'data_checked','highest_score','ranked_count','chip_plays',
                'most_selected','most_transferred_in','top_element','top_element_info','transfers_made',
                'most_captained','most_vice_captained']]
        
        # Convert the 'deadline_time' column to datetime (Pandas will automatically adjust for UTC)
        df['deadline_time'] = pd.to_datetime(df['deadline_time'])

        # format to 'YYYY-MM-DD HH:MM AM/PM'
        df['deadline_time'] = df['deadline_time'].dt.strftime('%Y-%m-%d %I:%M %p')

        # due to the columns having list fields convert to str to
        # prevent Oracle from throwing errors during insertion
        df['chip_plays'] = df['chip_plays'].apply(str)
        df['top_element_info'] = df['top_element_info'].apply(str)

        # Replace [] in columns with an empty string
        df['chip_plays'] = df['chip_plays'].apply(lambda x: '' if x == [] else x) 
        df['top_element_info'] = df['top_element_info'].apply(lambda x: '' if x == [] else x)

        # Replace NaN values with 0
        df = df.fillna(0)

        hash_columns = ['average_entry_score', 'finished', 'data_checked', 'highest_score']
        df['hash_value'] = df.apply(calculate_hash, columns=hash_columns, axis=1)

        df = df.rename(
            columns={'id':'gameweek_id', 'top_element':'top_player', 'top_element_info':'top_player_info'})

        return df



    # get player data
    def get_player_stat():

        df = get_data("elements", session) # Contains player stats
        if df.empty == False:
            print("Player data scrapped")
        # extract relevant columns
        df = df[['id','first_name', 'second_name', 'web_name', 'code', 'element_type', 'event_points', 
                'total_points', 'minutes', 'selected_by_percent',
                'form', 'photo', 'points_per_game', 'status', 'team', 'team_code', 'region', 
                'goals_scored', 'goals_conceded', 'assists', 'clean_sheets', 'own_goals', 'penalties_saved',
                'penalties_missed', 'yellow_cards', 'red_cards', 'saves', 'bonus', 'bps', 'influence', 
                'creativity', 'threat', 'ict_index', 'starts', 'expected_goals', 'expected_assists', 'expected_goal_involvements',
                'expected_goals_conceded']]
        
        # Replace NaN values with 0
        df = df.fillna(0)

        hash_columns = ['event_points', 'total_points', 'status', 'team']
        tqdm.pandas(desc="Calculating hash values for players data")
        df['hash_value'] = df.progress_apply(calculate_hash, columns=hash_columns, axis=1)

        df = df.rename(columns={'id':'player_id','team':'team_id', 'code':'player_code', 'element_type':'pos_id',
                                'minutes':'minutes_played','bonus':'total_bonus_pts'})
        
        return df



    # get player positions
    def get_positions():

        df = get_data("element_types", session) # Contains player positions
        if df.empty == False:
            print("Positions data scrapped")
        df = df[['id', 'plural_name', 'singular_name','singular_name_short', 'element_count']]
    
        hash_columns = ['singular_name_short', 'element_count']
        tqdm.pandas(desc="Calculating hash values for positions data")
        df['hash_value'] = df.progress_apply(calculate_hash, columns=hash_columns, axis=1)

        df = df.rename(columns={'id':'pos_id','singular_name_short':'position_name'}) # rename column

        return df


    # get teams info
    def get_team_stat():

        df = get_data("teams", session) # Contains team stats
        if df.empty == False:
            print("Teams data scrapped")

        df = df[['id', 'code', 'name','short_name', 'win', 'draw', 'loss', 'played', 'points',
        'position', 'strength','strength_overall_home', 'strength_overall_away',
        'strength_attack_home', 'strength_attack_away', 'strength_defence_home',
        'strength_defence_away']]
        
        hash_columns = ['name', 'short_name', 'strength']
        tqdm.pandas(desc="Calculating hash values for teams data")
        df['hash_value'] = df.progress_apply(calculate_hash, columns=hash_columns, axis=1)

        df = df.rename(columns={'id':'team_id', 'name':'team_name', 'short_name':'team_short_name'})

        return df

    # player_id_list = get_player_stat()['player_id'].to_list()

    def get_fixtures(player_ids):
        """
        Get data for multiple player_ids and store in  
        DataFrames for all remaining fixtures
        """
        all_fixtures = []
        
        # for player_id, player_data in tqdm(zip(player_ids, results), total=len(player_ids), desc="Processing player data"):
        for player_id in tqdm(player_ids, total=len(player_ids), desc="Fetching player fixture data"):
            player_data = fetch_player_data(player_id, session)
            
            if player_data is not None:
                # Extract 'fixtures', 'history', and 'history_past' if available
                fixtures = player_data.get('fixtures', [])
                
                # Append data to lists
                all_fixtures.extend(fixtures)
                
        # Convert lists to DataFrames
        df_fixtures = pd.DataFrame(all_fixtures)

        df_fixtures['kickoff_time'] = pd.to_datetime(df_fixtures['kickoff_time'])
        df_fixtures['kickoff_time'] = df_fixtures['kickoff_time'].dt.strftime('%Y-%m-%d %I:%M %p')
        
        """
        dropping duplicates because for players in the same team
        the fixtures will be duplicated
        """
        # df_fixtures.drop_duplicates(inplace=True) no need to drop dulicates

        return df_fixtures

    def get_history(player_ids):
        """
        Get data for multiple player_ids and store in  
        DataFrames for history. Data contains the past
        gameweeks for each player in the current season
        """
        all_history = []
        
        # for player_id, player_data in tqdm(zip(player_ids, results), total=len(player_ids), desc="Processing player data"):
        for player_id in tqdm(player_ids, total=len(player_ids), desc="Fetching player history data"):
            player_data = fetch_player_data(player_id, session)
            
            if player_data is not None:
                # Extract 'fixtures', 'history', and 'history_past' if available
                history = player_data.get('history', [])
                
                # Append data to lists
                all_history.extend(history)
                
        # Convert lists to DataFrames
        df_history = pd.DataFrame(all_history)

        df_history['kickoff_time'] = pd.to_datetime(df_history['kickoff_time'])
        df_history['kickoff_time'] = df_history['kickoff_time'].dt.strftime('%Y-%m-%d %I:%M %p')

        return df_history
    
    def get_history_past(player_ids):
        """
        Get data for multiple player_ids and store in  
        DataFrames for history_past.
        """
        all_history_past = []
        
        # for player_id, player_data in tqdm(zip(player_ids, results), total=len(player_ids), desc="Processing player data"):
        for player_id in tqdm(player_ids, total=len(player_ids), desc="Fetching player history past data"):
            player_data = fetch_player_data(player_id, session)
            
            if player_data is not None:
                # Extract 'fixtures', 'history', and 'history_past' if available
                history_past = player_data.get('history_past', [])
                
                # Append data to lists
                all_history_past.extend(history_past)
                
        # Convert lists to DataFrames
        df_history_past = pd.DataFrame(all_history_past)
        df_history_past['season_name'] = df_history_past['season_name'].str.replace('/', '-')
        # desired_pk_column = 'element_code'

        # Move the desired pk column to the front
        # df_history_past = df_history_past[[desired_pk_column] + [col for col in df_history_past.columns if col != desired_pk_column]]
       
        return df_history_past
    
    






