from matplotlib import pyplot as plt

from build_features import build_main_df
from consts import CLASSIFIERS
from create_classifiers import split_to_train_and_test, train_classifier, test_results
from process_data import get_users_data, get_processed_data, users_data_to_chunks

plt.style.use('ggplot')

def main():
    users_data = get_users_data()
    queries, times = users_data_to_chunks(users_data)

    all_domains_usage_count_df, domains_usage_count_df, valid_domains, suspicious_domains = get_processed_data(users_data)

    df = build_main_df(queries, times, all_domains_usage_count_df, domains_usage_count_df, valid_domains, suspicious_domains)
    train_x, train_y, test_x, test_y, test_y2 = split_to_train_and_test(df, queries)

    accuracy_results = []
    for classifier in CLASSIFIERS:
        models = train_classifier(train_x, train_y, classifier)
        accuracy = test_results(models, test_x, test_y, test_y2, classifier)
        accuracy_results.append(accuracy)

    names = [x.__name__ for x in CLASSIFIERS]
    plt.figure(figsize=(13, 13))
    plt.bar(names, accuracy_results)
    plt.show()

    print(accuracy_results)
    pass


if __name__ == "__main__":
    main()
