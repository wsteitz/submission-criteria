# System
"""Concordance Checking."""
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
    """Checks that the clustered submission data conforms to a concordance threshold

    Paramters:
    ----------
    P1 : ndarray
        Sorted validation submission probabilities based on the id

    P2 : ndarray
        Sorted test submission probabilities based on the id

    P3 : ndarray
        Sorted live submission probabilities based on the id

    c1 : ndarray
        Clustered validation from the tournament data

    c2 : ndarray
        Clustered test from the tournament data

    c3 : ndarray
        Clustered live from the tournament data

    threshold : float, optional, default: 0.12
        The threshold in which our mean ks_score has to be under to have "concordance"

    Returns:
    --------
    concordance : bool
        Boolean value of the clustered submission data having concordance
    """
    ks = []
    for i in set(c1):

        ks_score = max(ks_2samp(P1.reshape(-1)[c1==i], P2.reshape(-1)[c2==i])[0],
                       ks_2samp(P1.reshape(-1)[c1==i], P3.reshape(-1)[c3==i])[0],
                       ks_2samp(P3.reshape(-1)[c3==i], P2.reshape(-1)[c2==i])[0])

        ks.append(ks_score)
    logging.getLogger().info("Noticed score {}".format(np.mean(ks)))
    return np.mean(ks)<threshold

def make_clusters(X, X_1, X_2, X_3):
    """Split submission data into 3 clusters using K-Means clustering

    Parameters:
    -----------
    X: ndarray
        tournament data for the competition round

    X_1: ndarray
        sorted validation data ids from tournament data

    X_2: ndarray
        sorted test ids data from tournament data

    X_3: ndarray
        sorted live ids data from tournament data

    Returns:
    --------
    c1: nparray
        Clustered validation data
    c2: nparray
        Clustered test data
    c3: nparray
        Cluster live data
    """
    logging.getLogger().info("New competition, clustering dataset")
    kmeans = MiniBatchKMeans(n_clusters=5)

    kmeans.fit(X)
    c1, c2, c3 = kmeans.predict(X_1), kmeans.predict(X_2), kmeans.predict(X_3)
    logging.getLogger().info("Finished clustering")
    return c1, c2, c3

@functools.lru_cache(maxsize=2)
def get_ids(filemanager, competition_id):
    """Gets the ids from submission data based on the competition_id

    Parameters:
    -----------
    filemanager : FileManager
        S3 Bucket data access object for querying competition datasets
    competition_id : int
        The numerical id of the competition

    Returns:
    --------
    val : list
        List of all ids in the 'validation' dataset for the competition_id round

    test : list
        List of all ids in the 'test' dataset for the competition_id round

    live : list
        List of all ids in the 'live' dataset for the competition_id round
    """
    extract_dir = filemanager.download_dataset(competition_id)
    tournament = pd.read_csv(os.path.join(extract_dir, "numerai_tournament_data.csv"))
    val = tournament[tournament["data_type"] == "validation"]
    test = tournament[tournament["data_type"] == "test"]
    live = tournament[tournament["data_type"] == "live"]

    return list(val["id"]), list(test["id"]), list(live["id"])

def get_sorted_split(data, val_ids, test_ids, live_ids):
    """Split the competition data into validation, test, and live data sets in a sorted fashion

    Parameters:
    -----------
    data : DataFrame
        Tournament data for the competition round

    val_ids : list
        List of all validation data ids

    test_ids : list
        List of all test data ids

    live_ids : list
        List of all live data ids


    Returns:
    --------
    validation : ndarray
        Validation data features sorted by id

    test : ndarray
        Test data features sorted by id

    live : ndarray
        Live data features sorted by id
    """
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
    """Return the K-Means Clustered tournament data for the competition round

    Parameters:
    -----------
    competition_id : int
        Numerical ID of the competition round of the tournament

    db_manager : DatabaseManager
        MongoDB data access object that has read and write functions to NoSQL DB

    filemanager : FileManager
        S3 Bucket data access object for querying competition datasets

    Returns:
    --------
    variables : dictionary
        Holds clustered tournament data and the competition_id
    """
    extract_dir = filemanager.download_dataset(competition_id)

    training = pd.read_csv(os.path.join(extract_dir, "numerai_training_data.csv"))
    tournament = pd.read_csv(os.path.join(extract_dir, "numerai_tournament_data.csv"))

    val_ids, test_ids, live_ids = get_ids(filemanager, competition_id)
    return get_competition_variables_from_df(competition_id, training, tournament, val_ids, test_ids, live_ids)


def get_competition_variables_from_df(
        competition_id: str, training: pd.DataFrame, tournament: pd.DataFrame,
        val_ids: list, test_ids: list, live_ids: list) -> dict:

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
    """Get validation, test, and live ids sorted from submission_id

    Parameters:
    -----------
    submission_id : string
        ID of the submission

    competition_id : int
        Numerical ID of the competition round of the tournament

    db_manager : DatabaseManager
        MongoDB data access object that has read and write functions to NoSQL DB

    filemanager : FileManager
        S3 Bucket data access object for querying competition datasets

    Returns:
    --------
    validation : ndarray
        Sorted validation ids from submission data

    tests : ndarray
        Sorted test ids from submission data

    live : ndarray
        Sorted live ids from submission data
    """
    s3_file = db_manager.get_filename(submission_id)
    local_file = filemanager.download([s3_file])[0]
    data = pd.read_csv(local_file)
    val_ids, test_ids, live_ids = get_ids(filemanager, competition_id)
    validation, tests, live = get_sorted_split(data, val_ids, test_ids, live_ids)
    return validation, tests, live

def submission_concordance(submission, db_manager, filemanager):
    """Determine if a submission is concordant and write the result to MongoDB

    Parameters:
    -----------
    submission : dictionary
        Submission data that holds the ids of submission and competition round

    db_manager : DatabaseManager
        MongoDB data access object that has read and write functions to NoSQL DB

    filemanager : FileManager
            S3 Bucket data access object for querying competition datasets
    """
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
