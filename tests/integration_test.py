#!/usr/bin/env python
"""Integration testing."""

# System
import os

# Third Party
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis

# First Party
from testing_api import NumerAPI


data_dir = "numerai_datasets"
csv_dir = "test_csvs"
if not os.path.exists(csv_dir):
    os.makedirs(csv_dir)
TESTUSER = "xanderai"
DELAY = 10


def fetch_data(napi):
    if not os.path.exists(data_dir):
        print("Downloading the current dataset...")
        os.makedirs(data_dir)
        napi.download_current_dataset(dest_path=data_dir, unzip=True)
    else:
        print("Found old data to use.")


def load_data(path, frac=1):
    df = pd.read_csv(path)
    df.set_index("id", inplace=True)
    features = [f for f in df.columns if "feature" in f]
    # subsample the rows to speed things up
    df = df.sample(frac=frac)
    # subsample features to speed things up
    features = [f for f in features if int(f.strip("feature")) % 2 == 0]
    X, y = df[features], df["target"]
    return X, y, df['data_type']


def fit_clfs():

    path = '{}/numerai_training_data.csv'.format(data_dir)
    X, y, _ = load_data(path, frac=0.5)

    clfs = [RandomForestClassifier(n_estimators=100, n_jobs=-1, criterion='entropy'),
            # deactivated because too slow
            # GradientBoostingClassifier(learning_rate=0.05, subsample=0.5, max_depth=6, n_estimators=100),
            # KNeighborsClassifier(10, n_jobs=-1),
            DecisionTreeClassifier(max_depth=5),
            MLPClassifier(alpha=1, hidden_layer_sizes=(100, 100)),
            AdaBoostClassifier(),
            GaussianNB(),
            QuadraticDiscriminantAnalysis(),
            LogisticRegression(n_jobs=-1)]

    for clf in clfs:
        clf_str = str(clf).split("(")[0]
        print("Training a {}".format(clf_str))
        clf.fit(X, y)

    return clfs


def fetch_submission_status(napi):
    time.sleep(DELAY)

    leaderboard = napi.get_leaderboard()[0]['leaderboard']

    for user in leaderboard:
        if user['username'] == TESTUSER:
            concordant = "pending" if user['concordant']['pending'] else user["concordant"]["value"]
            original = "pending" if user['original']['pending'] else user["original"]["value"]

    return concordant, original


def predict(X, clf):
    y_prediction = clf.predict_proba(X)
    results = y_prediction[:, 1]
    df = pd.DataFrame(data={'prediction': results, 'id': X.index.values})
    return df


def to_str(clf):
    return str(clf).split("(")[0]


def check_single_models(clfs, napi):
    path = os.path.join(data_dir, 'numerai_tournament_data.csv')
    X, _, _ = load_data(path)

    for clf in clfs:
        df = predict(X, clf)
        out = os.path.join(csv_dir, "{}-legit.csv".format(to_str(clf)))
        print("Writing predictions to {}".format(out))
        # Save the predictions out to a CSV file
        df.to_csv(out, index=False)
        napi.upload_prediction(out)

        concordance, originality = fetch_submission_status(napi)
        assert concordance and originality


def check_concordance_fail(clfs, napi):
    path = os.path.join(data_dir, 'numerai_tournament_data.csv')
    X, _, datatype = load_data(path)

    X_valid = X[datatype == "validation"]
    X_test = X[datatype != "validation"]

    for i, clf1 in enumerate(clfs):
        for j, clf2 in enumerate(clfs):
            if i == j:
                continue

            valid_df = predict(X_valid, clf1)
            test_df = predict(X_test, clf2)
            mix = pd.concat([valid_df, test_df])

            filename = "{}-{}-mix.csv".format(to_str(clf1), to_str(clf2))
            out = os.path.join(csv_dir, filename)
            print("Writing predictions to {}".format(out))
            mix.to_csv(out, index=False)

            napi.upload_prediction(out)
            concordance, originality = fetch_submission_status(napi)
            # Concordance should fail
            assert concordance


def main():
    email = ""
    password = ""

    napi = NumerAPI()
    napi.credentials = (email, password)

    fetch_data(napi)
    clfs = fit_clfs()

    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    check_single_models(clfs, napi)
    check_concordance_fail(clfs, napi)


if __name__ == '__main__':
    main()
