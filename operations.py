import pandas as pd
import requests
from tqdm import tqdm
from base_scrapper import *

with requests.Session() as session:
        
    def get_gameweeks():
        """
        Retrieve Gameweeks data from Fantasy API
        """
        df = get_data("events", session) # Contains gameweek stats
        # if df.empty == False:
        #     print("Gameweeks data scrapped successfully")

        df = df[['id','name','deadline_time','deadline_time_epoch','average_entry_score','finished',
                'data_checked','highest_score','ranked_count','chip_plays',
                'most_selected','most_transferred_in','top_element','top_element_info','transfers_made',
                'most_captained','most_vice_captained']]
        
        df['deadline_time'] = pd.to_datetime(df['deadline_time'])
       
        df['deadline_time'] = df['deadline_time'].dt.strftime('%Y-%m-%d %I:%M %p')  # format to 'YYYY-MM-DD HH:MM AM/PM'
        
        """
        due to the columns having list fields convert to str to
        prevent Oracle from throwing errors during insertion
        """
        df['chip_plays'] = df['chip_plays'].apply(str)
        df['top_element_info'] = df['top_element_info'].apply(str)

        # Replace [] in columns with an empty string
        df['chip_plays'] = df['chip_plays'].apply(lambda x: '' if x == [] else x) 
        df['top_element_info'] = df['top_element_info'].apply(lambda x: '' if x == [] else x)

        df = df.fillna(0) # Replace NaN values with 0

        df = df.rename(
            columns={'id':'gameweek_id', 'top_element':'top_player', 'top_element_info':'top_player_info'})

        return df



    # get player data
    def get_player_stat():
        """
        Retrieve Players data from Fantasy API
        """
        df = get_data("elements", session) # Contains player stats
        # if df.empty == False:
        #     print("Player data scrapped successfully")

        # extract relevant columns
        df = df[['id','first_name', 'second_name', 'web_name', 'code', 'element_type', 'event_points', 
                'total_points', 'minutes', 'selected_by_percent',
                'form', 'photo', 'points_per_game', 'status', 'team', 'team_code', 'region', 
                'goals_scored', 'goals_conceded', 'assists', 'clean_sheets', 'own_goals', 'penalties_saved',
                'penalties_missed', 'yellow_cards', 'red_cards', 'saves', 'bonus', 'bps', 'influence', 
                'creativity', 'threat', 'ict_index', 'starts', 'expected_goals', 'expected_assists', 'expected_goal_involvements',
                'expected_goals_conceded']]
        
        df = df.fillna(0) # Replace NaN values with 0

        df = df.rename(columns={'id':'player_id','team':'team_id', 'code':'player_code', 'element_type':'pos_id',
                                'minutes':'minutes_played','bonus':'total_bonus_pts'})
        
        return df



    # get player positions
    def get_positions():
        """
        Retrieve Player positions data from Fantasy API
        """
        df = get_data("element_types", session) # Contains player positions
        # if df.empty == False:
        #     print("Positions data scrapped successfully")

        df = df[['id', 'plural_name', 'singular_name','singular_name_short', 'element_count']]
        df = df.rename(columns={'id':'pos_id','singular_name_short':'position_name'}) # rename column

        return df


    # get teams info
    def get_team_stat():
        """
        Retrieve Teams statistics data from Fantasy API
        """
        df = get_data("teams", session) # Contains team stats
        # if df.empty == False:
        #     print("Teams data scrapped successfully")

        df = df[['id', 'code', 'name','short_name', 'win', 'draw', 'loss', 'played', 'points',
        'position', 'strength','strength_overall_home', 'strength_overall_away',
        'strength_attack_home', 'strength_attack_away', 'strength_defence_home',
        'strength_defence_away']]

        df = df.rename(columns={'id':'team_id', 'name':'team_name', 'short_name':'team_short_name'})

        return df

    def get_player_ids():
        """
        Retrieve the IDs of players from player data
        The IDs are used to get individual player statistics and fixtures
        """
        df = get_data("elements", session)
        player_ids = df['id'].to_list()

        return player_ids

    def get_fixtures(player_ids):
        """
        Get players fixtures
        """
        all_fixtures = []
        
        for player_id in tqdm(player_ids, total=len(player_ids), desc="Fetching individual player fixture data"):
            player_data = fetch_player_data(player_id, session)
            
            if player_data is not None:
                fixtures = player_data.get('fixtures', [])

                # Append data to lists
                all_fixtures.extend(fixtures)
                
        # Convert lists to DataFrames
        df_fixtures = pd.DataFrame(all_fixtures)

        df_fixtures['kickoff_time'] = pd.to_datetime(df_fixtures['kickoff_time'])
        df_fixtures['kickoff_time'] = df_fixtures['kickoff_time'].dt.strftime('%Y-%m-%d %I:%M %p')
        
        return df_fixtures


    def get_history(player_ids):
        """
        Get past gameweeks stats for each player
        """
        
        all_history = []
        
        for player_id in tqdm(player_ids, total=len(player_ids), desc="Fetching individual player gameweek history data"):
            player_data = fetch_player_data(player_id, session)
            
            if player_data is not None:
                history = player_data.get('history', [])
                
                # Append data to lists
                all_history.extend(history)
                
        # Convert lists to DataFrames
        df_history = pd.DataFrame(all_history)

        df_history['kickoff_time'] = pd.to_datetime(df_history['kickoff_time'])
        df_history['kickoff_time'] = df_history['kickoff_time'].dt.strftime('%Y-%m-%d %I:%M %p')
        df_history = df_history.fillna(0) # Replace NaN values with 0

        return df_history
    
    def get_history_past(player_ids):
        """
        Get past season stats for individual players
        """
        all_history_past = []
        
        for player_id in tqdm(player_ids, total=len(player_ids), desc="Fetching individual player past seasons stats"):
            player_data = fetch_player_data(player_id, session)
            
            if player_data is not None:
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
    
    






