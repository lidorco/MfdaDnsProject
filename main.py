import os
import glob
import pandas as pd

from build_features import build_main_df
from data_exploration import explore_data

DATA_PATH = r".\data\raw\*.csv"

NUMBER_OF_QUERIES_EACH_CHUNK = 250
NUMBER_OF_USERS = 15
LEARNING_USERS = 10
TEST_UESR = 5


def collect_data():
    users_data = []
    for file in glob.glob(DATA_PATH):
        df = pd.DataFrame()
        df = df.from_csv(file)
        user_id = int(os.path.basename(file).split('.')[0].split('_user')[1])
        df['user'] = pd.Series([user_id] * len(df.index), index=df.index)
        users_data.append(df)
    return users_data


def merge_users_data_to_features_df(users_data):
    df = pd.concat([ user.loc[user['dns.flags.response'] == 0] for user in users_data]) # get only users queries
    df = df.reset_index()
    df = df.set_index(['user', 'frame.number'])
    return df






def main():
    users_data = collect_data()

    data_df = None # merge_users_data_to_features_df(users_data)
    domains_usage_count_df, valid_domains, suspicious_domains = explore_data(users_data)

    df = build_main_df(domains_usage_count_df, valid_domains, suspicious_domains)
    pass

if __name__ == "__main__":
    main()
