import numpy as np
import praw # type: ignore
from praw import Reddit # type: ignore
import sys
from utils.constants import POST_FIELDS
import pandas as pd # type: ignore

def connect_reddit(client_id, secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=secret,
            user_agent=user_agent
        )
        print('Connected to Reddit')
        return reddit
    except Exception as e:
        print(f'Error connecting to Reddit: {e}')
        sys.exit(1)
        
def extract_post(reddit: Reddit, subreddit: str, time_filter:str, limit=None):
    try:
        subreddit = reddit.subreddit(subreddit)
        posts = subreddit.top(time_filter=time_filter, limit=limit)
        post_list = []
        for post in posts:
           #convert post object to dictionary
           post_dict = vars(post)
           post = {key: post_dict[key] for key in post_dict if key in POST_FIELDS}
           post_list.append(post)
        return post_list
    except Exception as e:
        print(f'Error extracting posts: {e}')
        sys.exit(1)
           
def transform_data(post_df: pd.DataFrame):
    try:
        post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s').astype(str)
        post_df['over_18'] = np.where((post_df['over_18'] == True), True, False).astype(bool)
        post_df['author'] = post_df['author'].astype(str)
        edited_mode = post_df['edited'].mode()
        post_df['edited'] = np.where(
            (
                post_df['edited'].isin([True, False])
            ),
            post_df['edited'], #TRUE
            edited_mode #FALSE
        ).astype(bool)
        post_df['num_comments'] = post_df['num_comments'].astype(int)
        post_df['score'] = post_df['score'].astype(int)
        post_df['title'] = post_df['title'].astype(str)
        post_df['url'] = post_df['url'].astype(str)
        post_df['spoiler'] = post_df['spoiler'].astype(bool)
        post_df['stickied'] = post_df['stickied'].astype(bool)
        return  post_df
    
    except Exception as e:
        print(f'Error transforming data: {e}')
        sys.exit(1)
        
def load_data_to_csv(data: pd.DataFrame, path: str):
    try:
        data.to_csv(path, index=False)
        print('Data loaded to csv')
    except Exception as e:
        print(f'Error loading data to csv: {e}')
        sys.exit(1)