import copy
import datetime
import json
from functools import reduce

import pandas as pd

from consts import DOMAIN_COUNT_THRESHOLD, LEARNING_CHUNKS, NUMBER_OF_QUERIES_EACH_CHUNK


def explore_data(users_data):
    print("number of users: {}".format(len(users_data)))
    # check columns for each df
    print("number of columns: {}\n columns: {}\n".format(len(users_data[0].columns), users_data[0].columns))
    # check number of row on each df
    sum = 0
    min = 99999999
    max = 0
    for user in users_data:
        sum += len(user.index)
        if min > len(user.index):
            min = len(user.index)
        if max < len(user.index):
            max = len(user.index)
        # print("rows: {}".format(len(user.index)))
    print("rows avg: {}".format(sum / len(users_data)))
    print("rows min: {}".format(min))
    print("rows max: {}".format(max))

    return explore_domains(users_data)


def explore_domains(users_data):

    domains_per_user, valid_domains, suspicious_domains = basic_data_exploration(users_data)

    domains_usage_count = get_domains_usage_count(users_data, valid_domains)

    domains_usage_count_df = pd.DataFrame.from_dict(
        {'domains': list(domains_usage_count.keys()), 'usage': list(domains_usage_count.values())})
    domains_usage_count_df = domains_usage_count_df.sort_values('usage', ascending=False)

    # Avg of unique domains per user from valid domains:
    print("Number of valid domains {}. Examples:".format(len(valid_domains)))
    print(domains_usage_count_df.head())
    print("\n====Statistics with only valid domains:====\n")

    # Average of domains without duplication per user : 4236.
    domains_without_duplication_per_user = []
    for user in users_data:
        domains_without_duplication_per_user.append(len((set(user['dns.qry.name']) - suspicious_domains)))

    print("Average of domains without duplication per user : {}"
          .format(reduce(lambda x, y: x + y, domains_without_duplication_per_user) / len(users_data)))
    print("Min of domains without duplication per user : {}".format(min(domains_without_duplication_per_user)))
    print("Max of domains without duplication per user : {}".format(max(domains_without_duplication_per_user)))

    # Domain used only by specific user
    unique_domains_per_user_valid = []
    for index, user in enumerate(domains_per_user):
        user_domains = copy.deepcopy(user)
        user_domains -= suspicious_domains
        for u in domains_per_user[:index]:
            user_domains -= u

        for u in domains_per_user[index + 1:]:
            user_domains -= u

        unique_domains_per_user_valid.append(len(user_domains))
        # print("Unique domain for user {} is {}".format(index, len(user_domains)))

    print("Average of domains only used by specific users : {}"
          .format(reduce(lambda x, y: x + y, unique_domains_per_user_valid) / len(users_data)))
    print("Min of domains only used by specific user : {}".format(min(unique_domains_per_user_valid)))
    print("Max of domains only used by specific user : {}".format(max(unique_domains_per_user_valid)))

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

    # Remove domain used by all users from domains_usage_count_df
    everyone_domains_usage_count_df = pd.DataFrame(columns=['domains', 'usage'])
    for domain in valid_domains_used_by_all_users:
        everyone_domains_usage_count_df = pd.concat(
            [everyone_domains_usage_count_df, domains_usage_count_df.loc[domains_usage_count_df['domains'] == domain]])
        domains_usage_count_df.drop(domains_usage_count_df.loc[domains_usage_count_df['domains'] == domain].index,
                                    inplace=True)

    everyone_domains_usage_count_df = everyone_domains_usage_count_df.sort_values('usage', ascending=False)
    return domains_usage_count_df, valid_domains, suspicious_domains


def basic_data_exploration(users_data):
    domains_per_user = []
    for s in [user['dns.qry.name'].unique() for user in users_data]:
        domains_per_user.append(set(s))

    known_domains = pd.Series(reduce(lambda x, y: list(set(x).union(y)), domains_per_user))
    known_domains = known_domains.unique()

    print("Number of different domains in all data : {}".format(len(known_domains)))

    # avg of unique domains per user:
    sum = reduce(lambda x, y: x + y, [len(user['dns.qry.name'].unique()) for user in users_data])
    min_domains = reduce(lambda x, y: min(x, y), [len(user['dns.qry.name'].unique()) for user in users_data])
    max_domains = reduce(lambda x, y: max(x, y), [len(user['dns.qry.name'].unique()) for user in users_data])
    print("Average of domains without duplication per user : {}".format(sum / len(users_data)))
    print("Min of domains without duplication per user : {}".format(min_domains))
    print("Max of domains without duplication per user : {}".format(max_domains))

    # avg time for pcap per user
    users_sniff_time = [datetime.timedelta(seconds=int(user['frame.time_relative'].max())) for user in users_data]
    print("Average of sniff time per user : {}".format(
        reduce(lambda x, y: x + y, users_sniff_time) / len(users_sniff_time)))
    print("Min of sniff time per user : {}".format(min(users_sniff_time)))
    print("Max of sniff time per user : {}".format(max(users_sniff_time)))

    # domain used only by specific user
    sum = 0
    for index, user in enumerate(domains_per_user):
        user_domains = copy.deepcopy(user)
        for u in domains_per_user[:index]:
            user_domains -= u

        for u in domains_per_user[index + 1:]:
            user_domains -= u

        sum += len(user_domains)
        # print("Unique domain for user {} is {}".format(index, len(user_domains)))

    print("Average of domains only used by specific users : {}".format(sum / len(users_data)))

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
    common_domains_above_threshold = {key: value for key, value in common_domains.items() if value > 1}
    suspicious_domains = set(common_domains_without_dot.keys()) & set(common_domains_below_threshold.keys())
    valid_domains = set(common_domains.keys()) - suspicious_domains

    print("Number of suspicious domains - with less then {} usage and without dot: {}. Exmaples:\n{}\n"
          .format(DOMAIN_COUNT_THRESHOLD, len(suspicious_domains), list(suspicious_domains)[:5]))

    return domains_per_user, valid_domains, suspicious_domains



def get_domains_usage_count(users_data, valid_domains):
    domains_usage_count = {}

    all_packets = users_data[0].copy()
    for user in users_data[1:]:
        relevant_user_packet = user[:LEARNING_CHUNKS * NUMBER_OF_QUERIES_EACH_CHUNK] # comment this if you want to use queries from all data
        all_packets = all_packets.append(relevant_user_packet, ignore_index=True)

    all_relevant_packets = all_packets
    all_relevant_domains = all_relevant_packets['dns.qry.name'].value_counts().index
    domains_usage_in_relevant_packets = all_relevant_packets['dns.qry.name'].value_counts()

    if True:
        for domain in valid_domains:
            domains_usage_count[domain] = 0
            if domain in all_relevant_domains:
                domains_usage_count[domain] += int(domains_usage_in_relevant_packets[domain])

        with open("./data/domains_usage_count_only_train_data.json", 'w') as h:
            json.dump(domains_usage_count, h)
    else:
        with open("./data/domains_usage_count.json", 'r') as h:
            domains_usage_count = json.load(h)

    return domains_usage_count