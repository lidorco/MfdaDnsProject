from time import time

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score
from sklearn.utils import shuffle
from xgboost import XGBClassifier

from consts import NUMBER_OF_USERS, LEARNING_CHUNKS, TESTING_CHUNKS, USEABLE_CHUNKS


def create_classifiers(features, queries):
    ### Add labels ###:
    for user_id in range(NUMBER_OF_USERS):
        features['label{}'.format(user_id)] = \
            pd.Series([0] * USEABLE_CHUNKS * user_id + [1] * USEABLE_CHUNKS + [0] * USEABLE_CHUNKS * (NUMBER_OF_USERS - 1 - user_id),
                                                        index=features.index)

    ### create train_x train_y, test_x, test_y ###
    features = features.reset_index()

    train_x = features.loc[features['Chunk'] < LEARNING_CHUNKS].set_index(['User', 'Chunk'])  # features without labels (first 50)
    train_x = shuffle(train_x)
    train_y = pd.DataFrame({"label{}".format(i): list(train_x.pop("label{}".format(i))) for i in range(NUMBER_OF_USERS) }) # only labels (first 50)
    train_x = train_x.reset_index()
    train_x.pop('User')
    train_x.pop('Chunk')


    test_x = features.loc[features['Chunk'] >= LEARNING_CHUNKS].set_index(['User', 'Chunk'])  # features with label (last 40)
    test_y = pd.DataFrame({"label{}".format(i): list(test_x.pop("label{}".format(i))) for i in
                            range(NUMBER_OF_USERS)})  # only labels (last 40)
    test_y2 = pd.DataFrame(columns=['User', 'Chunk']).set_index(['User', 'Chunk']) # only labels (last 40)
    for user_id in range(NUMBER_OF_USERS):
        for chunk_id in range(LEARNING_CHUNKS, TESTING_CHUNKS + LEARNING_CHUNKS):
            test_y2.loc[(user_id, chunk_id), 'label'] = user_id
    test_x = test_x.reset_index()
    test_x.pop('User')
    test_x.pop('Chunk')


    ### train ###
    models = []
    t1 = time()
    print("Started XGBClassifier in", t1)
    for i in range(NUMBER_OF_USERS):
        model = XGBClassifier()
        model.fit(train_x, train_y['label{}'.format(i)])
        models.append(model)
    t2 = time()
    print('Finished XGBClassifier in ', t2, ' Total time ', t2 - t1, ' sec.')


    ### test results ###

    # check predictions:
    preds = []
    for i in range(NUMBER_OF_USERS):
        preds.append(models[i].predict_proba(test_x))

    # check accuracy 1:
    predictions = pd.DataFrame(columns=["label{}".format(x) for x in range(NUMBER_OF_USERS)])
    for chunk_number in range(TESTING_CHUNKS * NUMBER_OF_USERS):
        predicted_user_id = np.argmax([preds[user_id][:,1][chunk_number] for user_id in range(NUMBER_OF_USERS)])
        new_col = {"label{}".format(x):0 for x in range(NUMBER_OF_USERS)}
        new_col["label{}".format(predicted_user_id)] = 1
        predictions = predictions.append(new_col, ignore_index=True)

    predictions.eq(test_y.values).mean() # calculate accuracy for each user separately

    # check accuracy 2:
    predictions2 = pd.DataFrame(columns=['User', 'Chunk']).set_index(['User', 'Chunk'])
    for user_id in range(NUMBER_OF_USERS):
        for chunk_id in range(LEARNING_CHUNKS, TESTING_CHUNKS + LEARNING_CHUNKS):
            real_chunk_id = chunk_id-LEARNING_CHUNKS + user_id * TESTING_CHUNKS
            predicted_user_id = \
                np.argmax([preds[i][:,1][real_chunk_id] for i in range(NUMBER_OF_USERS)])
            predictions2.loc[(user_id, chunk_id), 'label'] = predicted_user_id

    accuracy = accuracy_score(predictions2, test_y2)
    print("Accuracy: %.2f%%" % (accuracy * 100.0))
    pass
