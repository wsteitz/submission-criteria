# System
import logging
import os
import functools

# Third Party
from sklearn.cluster import MiniBatchKMeans
from scipy.stats import ks_2samp
import numpy as np
import pandas as pd
from bson.objectid import ObjectId

def has_concordance(P1, P2, P3, c1, c2, c3, threshold=0.12):
    ks = []
    for i in set(c1):

        ks_score = max(ks_2samp(P1.reshape(-1)[c1==i], P2.reshape(-1)[c2==i])[0],
                       ks_2samp(P1.reshape(-1)[c1==i], P3.reshape(-1)[c3==i])[0],
                       ks_2samp(P3.reshape(-1)[c3==i], P2.reshape(-1)[c2==i])[0])

        ks.append(ks_score)
    logging.getLogger().info("Noticed score {}".format(np.mean(ks)))
    return np.mean(ks)<threshold

def make_clusters(X, X_1, X_2, X_3):
    logging.getLogger().info("New competition, clustering dataset")
    kmeans = MiniBatchKMeans(n_clusters=5)

    kmeans.fit(X)
    c1, c2, c3 = kmeans.predict(X_1), kmeans.predict(X_2), kmeans.predict(X_3)
    logging.getLogger().info("Finished clustering")
    return c1, c2, c3

@functools.lru_cache(maxsize=2)
def get_ids(filemanager, competition_id):
    extract_dir = filemanager.download_dataset(competition_id)
    tournament = pd.read_csv(os.path.join(extract_dir, "numerai_tournament_data.csv"))
    val = tournament[tournament["data_type"] == "validation"]
    test = tournament[tournament["data_type"] == "test"]
    live = tournament[tournament["data_type"] == "live"]

    return list(val["id"]), list(test["id"]), list(live["id"])

def get_sorted_split(data, val_ids, test_ids, live_ids):
    validation = data[data["id"].isin(val_ids)]
    test = data[data["id"].isin(test_ids)]
    live = data[data["id"].isin(live_ids)]

    validation = validation.sort_values("id")
    test = test.sort_values("id")
    live = live.sort_values("id")

    if any(["feature" in c for c in list(validation)]):
        f = [c for c in list(validation) if "feature" in c]
    else:
        f = ["probability"]
    validation = validation[f]
    test = test[f]
    live = live[f]

    return validation.as_matrix(), test.as_matrix(), live.as_matrix()

@functools.lru_cache(maxsize=2)
def get_competition_variables(competition_id, db_manager, filemanager):
    extract_dir = filemanager.download_dataset(competition_id)

    training = pd.read_csv(os.path.join(extract_dir, "numerai_training_data.csv"))
    tournament = pd.read_csv(os.path.join(extract_dir, "numerai_tournament_data.csv"))

    val_ids, test_ids, live_ids = get_ids(filemanager, competition_id)

    f = [c for c in list(tournament) if "feature" in c]
    # TODO the dropna is a hack workaround for https://github.com/numerai/api-ml/issues/68
    X = training[f].dropna().as_matrix()
    X = np.append(X, tournament[f].as_matrix(), axis=0)

    X_1, X_2, X_3 = get_sorted_split(tournament, val_ids, test_ids, live_ids)
    c1, c2, c3 = make_clusters(X, X_1, X_2, X_3)

    variables = {
        "competition_id":competition_id,
        "cluster_1" : c1,
        "cluster_2" : c2,
        "cluster_3" : c3,
    }
    return variables

def get_submission_pieces(submission_id, competition_id,  db_manager, filemanager):
    s3_file = db_manager.get_filename(submission_id)
    local_file = filemanager.download([s3_file])[0]
    data = pd.read_csv(local_file)
    val_ids, test_ids, live_ids = get_ids(filemanager, competition_id)
    validation, tests, live = get_sorted_split(data, val_ids, test_ids, live_ids)
    return validation, tests, live

def submission_concordance(submission, db_manager, filemanager):
    s = db_manager.db.submissions.find_one({'_id':ObjectId(submission["submission_id"])})
    submission['user'] = s['username']
    submission['competition_id'] = s['competition_id']

    clusters = get_competition_variables(submission['competition_id'], db_manager, filemanager)
    P1, P2, P3 = get_submission_pieces(submission['submission_id'], submission['competition_id'], db_manager, filemanager)
    c1, c2, c3 = clusters["cluster_1"], clusters["cluster_2"], clusters["cluster_3"]

    try:
        concordance = has_concordance(P1, P2,P3, c1, c2, c3)
    except IndexError:
        # If we had an indexing error, that is because the round restart, and we need to try getting the new competition variables.
         get_competition_variables.cache_clear()
         clusters = get_competition_variables(submission['competition_id'], db_manager, filemanager)
         c1, c2, c3 = clusters["cluster_1"], clusters["cluster_2"], clusters["cluster_3"]
         concordance = has_concordance(P1, P2,P3, c1, c2, c3)

    db_manager.write_concordance(submission['submission_id'], submission['competition_id'], concordance)
