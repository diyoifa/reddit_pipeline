from utils.constants import CLIENT_ID, SECRET, INPUT_PATH, OUTPUT_PATH
from etls.reddit_etl import connect_reddit, extract_post, transform_data, load_data_to_csv
import pandas as pd # type: ignore


def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    #connect to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'diyoifa/1.0 (by test1002542235@gmail.com)')
    #extract data
    posts = extract_post(instance, subreddit, time_filter, limit)
    #transform data
    post_df = pd.DataFrame(posts)
    post_df = transform_data(post_df)
    #load data to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)
    return file_path