
import pandas as pd
import os
from datetime import datetime

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

def fetch_and_save_season_data(season, cursor):
    # SQL query to get data for the specified season
    query = f"""
        select first_name || ' ' || second_name as name,
            team_name team, position_name position,
            hp.season_name season,
            hp.start_cost, hp.end_cost, hp.total_points,
            hp.minutes, hp.goals_scored, hp.assists,
            hp.clean_sheets, hp.goals_conceded, hp.own_goals,
            hp.peNalties_saved, hp.penalties_missed, hp.yellow_cards,
            hp.red_cards, hp.saves, hp.bonus, hp.bps,hp.influence,
            hp.creativity, hp.threat, hp.ict_index, hp.starts,
            hp.expected_goals, hp.expected_assists, hp.expected_goal_involvements,
            hp.expected_goals_conceded
            from PLAYERS p
    JOIN HISTORY_PAST hp ON p.player_code = hp.element_code
    JOIN TEAMS t ON t.team_id = p.team_id
    JOIN POSITIONS pos ON pos.pos_id = p.pos_id
    where hp.season_name = :season
    """
    
    cursor.execute(query, [season])
    
    columns = [col[0] for col in cursor.description]  # Get column names
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    
    # Create the directory if it doesn't exist
    directory = f"data/season/{season}"
    os.makedirs(directory, exist_ok=True)
    
    # Save the DataFrame to a CSV file
    file_path = f"{directory}/season_stats.{timestamp}.csv"
    df.to_csv(file_path, index=False)
    print(f"Data for {season} saved to {file_path}")


def player_season_data(season, cursor):
    # SQL query to generate players stats for diffferent seasons
    query = f"""
        select p.WEB_NAME,p.PLAYER_ID,p.POS_ID,p.PHOTO,
        h.* from history_past h
        join players p
        on p.PLAYER_CODE = h.ELEMENT_CODE
        where season_name = :season
    """
    cursor.execute(query, [season])
    
    columns = [col[0] for col in cursor.description]  # Get column names
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    
    # Create the directory if it doesn't exist
    directory = f"data/players/{season}"
    os.makedirs(directory, exist_ok=True)
    
    # Save the DataFrame to a CSV file
    file_path = f"{directory}/players_{season}_stats.csv"
    df.to_csv(file_path, index=False)
    print(f"Data for {season} saved to {file_path}")


def player_current_season_data(cursor):
    # SQL query to generate players stats for diffferent seasons
    query = f"""
        select p.first_name || ' ' || p.second_name as name,p.WEB_NAME,
            pos.POSITION_NAME,p.PHOTO,gws.name gameweek,
            gws.DEADLINE_TIME,
            t.TEAM_NAME opponent,
            h.TOTAL_POINTS,
            h.WAS_HOME, h.KICKOFF_TIME,
            h.TEAM_A_SCORE, h.TEAM_H_SCORE,
            h.MINUTES, h.GOALS_SCORED,
            h.ASSISTS, h.CLEAN_SHEETS, h.GOALS_CONCEDED,
            h.OWN_GOALS, h.PENALTIES_SAVED, h.PENALTIES_MISSED,
            h.YELLOW_CARDS, h.RED_CARDS, h.SAVES,
            h.BONUS, h.BPS, h.INFLUENCE, h.CREATIVITY,
            h.THREAT, h.ICT_INDEX, h.STARTS, h.EXPECTED_GOALS,
            h.EXPECTED_ASSISTS, h.EXPECTED_GOAL_INVOLVEMENTS,
            h.EXPECTED_GOALS_CONCEDED, h.VALUE, h.TRANSFERS_BALANCE,
            h.SELECTED, h.TRANSFERS_IN, h.TRANSFERS_OUT
        from history h
        join players p
            on p.PLAYER_ID = h.ELEMENT
        join gameweeks gws
            on gws.gameweek_id = h.round
        join positions pos
            on pos.POS_ID = p.pos_id
        join teams t
            on t.team_id = h.opponent_team
        order by p.WEB_NAME, gws.NAME
    """
    cursor.execute(query)
    
    columns = [col[0] for col in cursor.description]  # Get column names
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    
    # Create the directory if it doesn't exist
    directory = f"data"
    os.makedirs(directory, exist_ok=True)
    
    # Save the DataFrame to a CSV file
    file_path = f"{directory}/current_season_stats.csv"
    df.to_csv(file_path, index=False)
    print(f"Current season data saved to {file_path}")