import glob
import os
from functools import reduce

import pandas as pd

from consts import DOMAIN_COUNT_THRESHOLD, LEARNING_CHUNKS, NUMBER_OF_QUERIES_EACH_CHUNK, DATA_PATH


def users_data_to_chunks(users_data):
    """Returns:
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

def get_users_data():
    """
    Returns: list of DataFrame, each df represent user traffic info
    """
    users_data = []
    for file in glob.glob(DATA_PATH):
        df = pd.DataFrame()
        df = df.from_csv(file)
        user_id = int(os.path.basename(file).split('.')[0].split('_user')[1])
        df['user'] = pd.Series([user_id] * len(df.index), index=df.index)
        users_data.append(df)
    return users_data


def get_processed_data(users_data):
    domains_per_user, valid_domains, suspicious_domains = extract_domains(users_data)

    domains_usage_count = get_domains_usage_count(users_data, valid_domains)

    domains_usage_count_df = pd.DataFrame.from_dict(
        {'domains': list(domains_usage_count.keys()), 'usage': list(domains_usage_count.values())})
    domains_usage_count_df = domains_usage_count_df.sort_values('usage', ascending=False)

    # Valid domains which all users use:
    valid_domains_used_by_all_users = set()
    for domain in valid_domains:
        everyone_use_this_domain = True
        for user in domains_per_user:
            if domain not in user:
                everyone_use_this_domain = False
                break
        if everyone_use_this_domain:
            valid_domains_used_by_all_users.add(domain)

    all_domains_usage_count_df = domains_usage_count_df.copy()
    # Remove domain used by all users from domains_usage_count_df
    everyone_domains_usage_count_df = pd.DataFrame(columns=['domains', 'usage'])
    for domain in valid_domains_used_by_all_users:
        everyone_domains_usage_count_df = pd.concat(
            [everyone_domains_usage_count_df, domains_usage_count_df.loc[domains_usage_count_df['domains'] == domain]])
        domains_usage_count_df.drop(domains_usage_count_df.loc[domains_usage_count_df['domains'] == domain].index,
                                    inplace=True)

    everyone_domains_usage_count_df = everyone_domains_usage_count_df.sort_values('usage', ascending=False)
    return all_domains_usage_count_df, domains_usage_count_df, valid_domains, suspicious_domains


def extract_domains(users_data):
    """
    Returns tuple (domains_per_user, valid_domains, suspicious_domains):
        domains_per_user: list of sets. each set contain domains used by specific user.
        valid_domains: set of domains which considered valid.
        suspicious_domains set of domain which considered suspicious.
    """
    domains_per_user = []
    for s in [user['dns.qry.name'].unique() for user in users_data]:
        domains_per_user.append(set(s))

    known_domains = pd.Series(reduce(lambda x, y: list(set(x).union(y)), domains_per_user))
    known_domains = known_domains.unique()

    # top domain usages:
    all_domains = []
    for s in [user['dns.qry.name'].unique() for user in users_data]:
        all_domains.extend(s)

    common_domains = {}
    for domain in known_domains:
        common_domains[domain] = len([user for user in domains_per_user if domain in user])

    common_domains_below_threshold = {key: value for key, value in common_domains.items() if
                                      value <= DOMAIN_COUNT_THRESHOLD}
    common_domains_without_dot = {key: value for key, value in common_domains.items() if '.' not in key}
    suspicious_domains = set(common_domains_without_dot.keys()) & set(common_domains_below_threshold.keys())
    valid_domains = set(common_domains.keys()) - suspicious_domains

    return domains_per_user, valid_domains, suspicious_domains


def get_domains_usage_count(users_data, valid_domains):
    domains_usage_count = {}

    all_packets = users_data[0][:LEARNING_CHUNKS * NUMBER_OF_QUERIES_EACH_CHUNK].copy()
    for user in users_data[1:]:
        relevant_user_packet = user[:LEARNING_CHUNKS * NUMBER_OF_QUERIES_EACH_CHUNK] # comment this if you want to use queries from all data
        all_packets = all_packets.append(relevant_user_packet, ignore_index=True)

    all_relevant_packets = all_packets
    all_relevant_domains = all_relevant_packets['dns.qry.name'].value_counts().index
    domains_usage_in_relevant_packets = all_relevant_packets['dns.qry.name'].value_counts()

    for domain in valid_domains:
        domains_usage_count[domain] = 0
        if domain in all_relevant_domains:
            domains_usage_count[domain] += int(domains_usage_in_relevant_packets[domain])

    return domains_usage_count
