from time import time

import pandas as pd
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier

from consts import NUMBER_OF_USERS, LEARNING_CHUNKS, TESTING_CHUNKS, USEABLE_CHUNKS


def create_classifiers(features, queries):
    #Add labels:
    for user_id in range(NUMBER_OF_USERS):
        features['label{}'.format(user_id)] = \
            pd.Series([0] * USEABLE_CHUNKS * user_id + [1] * USEABLE_CHUNKS + [0] * USEABLE_CHUNKS * (NUMBER_OF_USERS - 1 - user_id),
                                                        index=features.index)

    # create train_x train_y, test_x, test_y
    features = features.reset_index()

    train_x = features.loc[features['Chunk'] < LEARNING_CHUNKS].set_index(['User', 'Chunk'])  # features without labels (first 50)
    train_y = pd.DataFrame({"label{}".format(i): list(train_x.pop("label{}".format(i))) for i in range(NUMBER_OF_USERS) }) # only labels (first 50)
    train_x = train_x.reset_index()
    train_x.pop('User')
    train_x.pop('Chunk')


    test_x = features.loc[features['Chunk'] >= LEARNING_CHUNKS].set_index(['User', 'Chunk'])  # features with label (last 40)
    test_y = pd.DataFrame({"label{}".format(i): list(test_x.pop("label{}".format(i))) for i in
                            range(NUMBER_OF_USERS)})  # only labels (last 40)
    test_x = test_x.reset_index()
    test_x.pop('User')
    test_x.pop('Chunk')

    # train
    models = []
    t1 = time()
    print("Started XGBClassifier in", t1)
    for i in range(NUMBER_OF_USERS):
        model = XGBClassifier()
        model.fit(train_x, train_y['label{}'.format(i)])
        models.append(model)
    t2 = time()
    print('Finished XGBClassifier in ', t2, ' Total time ', t2 - t1, ' sec.')

    # test results
    preds = []
    for i in range(NUMBER_OF_USERS):
        preds.append(models[i].predict_proba(test_x))

    pass # TODO: calculate final prediction results and prediction rate using test_y
