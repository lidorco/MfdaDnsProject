import pandas as pd

from consts import USEABLE_CHUNKS, COMMON_DOMAIN_COUNT


def build_main_df(queries, domains_usage_count_df, valid_domains, suspicious_domains):
    """
    Returns df which indexes is: (user-id chunk-number)
    """
    df = pd.DataFrame(columns=['User', 'Chunk']).set_index(['User', 'Chunk'])

    # common domains - number of appearance of most COMMON_DOMAIN_COUNT domains(which not everyone use)
    top_100_common_domains = list(domains_usage_count_df.iloc[:COMMON_DOMAIN_COUNT]['domains'])
    top_100_common_domains = set(top_100_common_domains)
    for user_id in range(len(queries)):
        for chunk_id in range(len(queries[user_id])):
            for domain in top_100_common_domains:
                df.loc[(user_id, chunk_id), domain] = queries[user_id][chunk_id].count(domain)

    # remove data not needed:
    df = df.reset_index()
    df = df.loc[df['Chunk'] < USEABLE_CHUNKS]
    df = df.set_index(['User', 'Chunk'])
    return df
