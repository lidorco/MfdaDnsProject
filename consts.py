from xgboost import XGBClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import LinearSVC # AttributeError:'LinearSVC' object has no attribute 'predict_proba'
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import SelectKBest # AttributeError: 'SelectKBest' object has no attribute 'predict_proba'
from sklearn.gaussian_process import GaussianProcessClassifier # Bad prediction (7%)
from sklearn.neural_network import MLPClassifier

DATA_PATH = r".\data\raw\*.csv"

TOP_DOMAIN_USAGES = 100
DOMAIN_COUNT_THRESHOLD = 1

NUMBER_OF_QUERIES_EACH_CHUNK = 750
NUMBER_OF_USERS = 15

LEARNING_CHUNKS = 50
TESTING_CHUNKS = 40
USEABLE_CHUNKS = LEARNING_CHUNKS + TESTING_CHUNKS

CLASSIFIERS = [XGBClassifier, GaussianNB, KNeighborsClassifier, AdaBoostClassifier, GradientBoostingClassifier,
               RandomForestClassifier, MLPClassifier]
ENABLE_COMMON_DOMAIN_FEATURE = True
COMMON_DOMAIN_COUNT = 100

SECONDS_IN_MINUTES = 60