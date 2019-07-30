import pandas as pd

from consts import USEABLE_CHUNKS, COMMON_DOMAIN_COUNT, NUMBER_OF_QUERIES_EACH_CHUNK, SECONDS_IN_MINUTES


def build_main_df(queries, times, all_domains_usage_count_df, domains_usage_count_df, valid_domains, suspicious_domains):
    """
    Returns df which indexes is: (user-id chunk-number)
    """
    df1 = common_domains_not_by_everyone(queries, domains_usage_count_df, valid_domains, suspicious_domains)
    #df2 = common_domains(queries, all_domains_usage_count_df, valid_domains, suspicious_domains)
    #df3 = numaric_statistics(queries, times, all_domains_usage_count_df, valid_domains, suspicious_domains)
    df4 = time_statistics(queries, times, all_domains_usage_count_df, valid_domains, suspicious_domains)

    df = df1.join(df4)

    # remove data not needed:
    df = df.reset_index()
    df = df.loc[df['Chunk'] < USEABLE_CHUNKS]
    df = df.set_index(['User', 'Chunk'])

    return df


def common_domains_not_by_everyone(queries, domains_usage_count_df, valid_domains, suspicious_domains):
    """
    Returns feature df which is - the most commons domains which NOT used by all users.
    """
    df = pd.DataFrame(columns=['User', 'Chunk']).set_index(['User', 'Chunk'])

    # common domains - number of appearance of most COMMON_DOMAIN_COUNT domains(which not everyone use)
    top_100_common_domains = list(domains_usage_count_df.iloc[:COMMON_DOMAIN_COUNT]['domains'])
    top_100_common_domains = set(top_100_common_domains)
    for user_id in range(len(queries)):
        for chunk_id in range(len(queries[user_id])):
            for domain in top_100_common_domains:
                df.loc[(user_id, chunk_id), domain] = queries[user_id][chunk_id].count(domain)

    return df


def common_domains(queries, all_domains_usage_count_df, valid_domains, suspicious_domains):
    """
    Returns feature df which is - the most commons domains.
    """
    df = pd.DataFrame(columns=['User', 'Chunk']).set_index(['User', 'Chunk'])

    # common domains - number of appearance of most COMMON_DOMAIN_COUNT domains
    top_100_common_domains = list(all_domains_usage_count_df.iloc[:COMMON_DOMAIN_COUNT]['domains'])
    top_100_common_domains = set(top_100_common_domains)
    for user_id in range(len(queries)):
        for chunk_id in range(len(queries[user_id])):
            for domain in top_100_common_domains:
                df.loc[(user_id, chunk_id), domain] = queries[user_id][chunk_id].count(domain)

    return df


def numaric_statistics(queries, times, all_domains_usage_count_df, valid_domains, suspicious_domains):
    df = pd.DataFrame(columns=['User', 'Chunk']).set_index(['User', 'Chunk'])

    # Amount of different queries
    for user_id in range(len(queries)):
        for chunk_id in range(len(queries[user_id])):
            df.loc[(user_id, chunk_id), 'different_queries_count'] = len(set(queries[user_id][chunk_id]))

    # Time from start to end (entire chunk sniffing time in minutes)
    for user_id in range(len(queries)):
        for chunk_id in range(len(queries[user_id])):
            df.loc[(user_id, chunk_id), 'chunk_sniffing_time_in_minutes'] = \
                (times[user_id][chunk_id][len(times[user_id][chunk_id])-1] - times[user_id][chunk_id][0]) / SECONDS_IN_MINUTES

    return df


def time_statistics(queries, times, all_domains_usage_count_df, valid_domains, suspicious_domains):
    df = pd.DataFrame(columns=['User', 'Chunk']).set_index(['User', 'Chunk'])

    for user_id in range(len(queries)):

        for chunk_id in range(len(queries[user_id])):
            time_between_queries_sum = 0
            time_between_queries_min = 1000
            time_between_queries_max = 0

            # Avg time between queries
            # Min/Max time between queries
            queries_count_in_chunk = len(queries[user_id][chunk_id])
            for query_time_index in range(queries_count_in_chunk-1):
                time_between_queries = times[user_id][chunk_id][query_time_index + 1] - times[user_id][chunk_id][query_time_index]
                time_between_queries_sum += time_between_queries
                if time_between_queries > time_between_queries_max:
                    time_between_queries_max = time_between_queries
                if time_between_queries < time_between_queries_min:
                    time_between_queries_min = time_between_queries

            time_between_queries_avg = time_between_queries_sum / float((NUMBER_OF_QUERIES_EACH_CHUNK - 1))
            df.loc[(user_id, chunk_id), 'avg_seconds_between_queries'] = time_between_queries_avg
            df.loc[(user_id, chunk_id), 'min_seconds_between_queries'] = time_between_queries_min
            df.loc[(user_id, chunk_id), 'max_seconds_between_queries'] = time_between_queries_max

            # Standard deviation time between queries
            std_dev_sum = 0
            for query_time_index in range(queries_count_in_chunk - 1):
                time_between_queries = times[user_id][chunk_id][query_time_index + 1] - times[user_id][chunk_id][query_time_index + 1]
                std_dev_sum += (time_between_queries - time_between_queries_avg)**2

            time_between_queries_std_dev = std_dev_sum / float((NUMBER_OF_QUERIES_EACH_CHUNK - 1))
            df.loc[(user_id, chunk_id), 'std_dev_seconds_between_queries'] = time_between_queries_std_dev

    return df
