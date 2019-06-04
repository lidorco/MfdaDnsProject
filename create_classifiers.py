from time import time

import pandas as pd
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier

from consts import NUMBER_OF_USERS, LEARNING_CHUNKS, TESTING_CHUNKS


def create_classifiers(features, queries):
    # create train_x train_y, test_x, test_y
    features = features.reset_index()
    train_x = features.loc[features['Chunk'] < 50].set_index(['User', 'Chunk'])  # features without labels (first 50)
    train_y = pd.DataFrame(columns=['User', 'Chunk']).set_index(['User', 'Chunk'])  # only labels (first 50)
    for user_id in range(NUMBER_OF_USERS):
        for chunk_id in range(LEARNING_CHUNKS):
            train_y.loc[(user_id, chunk_id), 'label'] = user_id

    test_x = features.loc[features['Chunk'] >= 50].set_index(['User', 'Chunk'])  # features with label (last 40)
    test_y = pd.DataFrame(columns=['User', 'Chunk']).set_index(['User', 'Chunk'])  # only labels (last 40)
    for user_id in range(NUMBER_OF_USERS):
        for chunk_id in range(LEARNING_CHUNKS, TESTING_CHUNKS + LEARNING_CHUNKS):
            test_y.loc[(user_id, chunk_id), 'label'] = user_id

    features.set_index(['User', 'Chunk'])

    # train
    model = XGBClassifier()
    t1 = time()
    print("Started XGBClassifier in", t1)
    model.fit(train_x, train_y)
    t2 = time()
    print('Finished XGBClassifier in ', t2, ' Total time ', t2 - t1, ' sec.')

    # test results
    pred_y = model.predict(test_x)
    predictions = [round(value) for value in pred_y]
    accuracy = accuracy_score(test_y, predictions)
    print("Accuracy: %.2f%%" % (accuracy * 100.0))
