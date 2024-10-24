import json
import pandas as pd
from requests.exceptions import HTTPError

import logging
logging.basicConfig(level=logging.INFO)

# base url
def get_data(top_level_key, session, timeout=10):
    """
    Function to scrap data from Fanatsy API

    Args:
        top_level_key: available keys eg. 'elements' for player data

    Returns:
        A pandas dataframe
    """
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()  # raise http error if one occurs

        data = response.json()

        # Extract your desired data
        available_keys = data.keys()
        my_data = data.get(top_level_key, [])

        if not my_data:
            print(f"Invalid key. Available keys are: {available_keys}")
            raise ValueError("No data found in the response.")

        df = pd.DataFrame(my_data)

        return df
    
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        logging.error(f"HTTP error occurred: {http_err}")
    
        # If something went wrong, return an empty DataFrame
        return pd.DataFrame()


def fetch_player_data(player_id, session):
    """
    Function for fetching individual player data (fixtures, history, history_past)
    
    Args:
        Individual player ids and session

    Returns:
        pandas dataframe
    """
    base_url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
    response = session.get(base_url)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(f"Failed to fetch data for player_id {player_id}. Status code: {response.status_code}")
        return None










