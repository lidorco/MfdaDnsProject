import glob
import os

import pandas as pd

from build_features import build_main_df
from consts import DATA_PATH, NUMBER_OF_QUERIES_EACH_CHUNK
from create_classifiers import create_classifiers
from data_exploration import explore_data


def collect_data():
    users_data = []
    for file in glob.glob(DATA_PATH):
        df = pd.DataFrame()
        df = df.from_csv(file)
        user_id = int(os.path.basename(file).split('.')[0].split('_user')[1])
        df['user'] = pd.Series([user_id] * len(df.index), index=df.index)
        users_data.append(df)
    return users_data


def users_data_to_chunks(users_data):
    """returns:
    [
        [['www.google.com', 'www.facebook.com', ...], [], [], ... []] - user 1
        [[], [], [], ... []] - user 2
        [[], [], [], ... []] - user 3
        ..
        [[], [], [], ... []] - user 15
    ]
    """
    users_queries = []
    for user in users_data:
        users_queries.append(list(user.loc[user['dns.flags.response'] == 0]['dns.qry.name']))

    users_queries_split_by_chunks = []
    for user_queries in users_queries:
        number_of_chunks = len(user_queries)
        user_queries_split_by_chunks = []
        for i in range(int(number_of_chunks / NUMBER_OF_QUERIES_EACH_CHUNK) + 1):
            user_queries_split_by_chunks.append(
                user_queries[i * NUMBER_OF_QUERIES_EACH_CHUNK:(1 + i) * NUMBER_OF_QUERIES_EACH_CHUNK])
        users_queries_split_by_chunks.append(user_queries_split_by_chunks)

    return users_queries_split_by_chunks


def main():
    users_data = collect_data()

    domains_usage_count_df, valid_domains, suspicious_domains = explore_data(users_data)
    queries = users_data_to_chunks(users_data)

    df = build_main_df(queries, domains_usage_count_df, valid_domains, suspicious_domains)
    create_classifiers(df, queries)
    pass


if __name__ == "__main__":
    main()
