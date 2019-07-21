from build_features import build_main_df
from create_classifiers import create_classifiers, train_classifier, test_results
from process_data import get_users_data, get_processed_data, users_data_to_chunks


def main():
    users_data = get_users_data()
    queries = users_data_to_chunks(users_data)

    domains_usage_count_df, valid_domains, suspicious_domains = get_processed_data(users_data)

    df = build_main_df(queries, domains_usage_count_df, valid_domains, suspicious_domains)
    train_x, train_y, test_x, test_y, test_y2 = create_classifiers(df, queries)
    models = train_classifier(train_x, train_y)
    test_results(models, test_x, test_y, test_y2)


if __name__ == "__main__":
    main()
