
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


