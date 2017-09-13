# System
import logging
import functools
from threading import Lock

# Third Party
from scipy.stats import ks_2samp
from scipy.stats.stats import pearsonr
import numpy as np
import pandas as pd
from bson.objectid import ObjectId

lock = Lock()

@functools.lru_cache(maxsize=512)
def get_submission(db_manager, filemanager, submission_id):
    if not submission_id:
        return None

    s3_filename = db_manager.get_filename(submission_id)
    try:

        local_files = filemanager.download([s3_filename])
        if len(local_files) != 1:
            logging.getLogger().info("Error looking for submission {}, found files".format(submission_id, local_files))
            return None

        local_file = local_files[0]
    except Exception as e:
        logging.getLogger().info("Could not get submission {}".format(submission_id))
        return None

    df = pd.read_csv(local_file)
    df.sort_values("id", inplace=True)
    df = df["probability"]
    return df.as_matrix()

def original(submission1, submission2, threshold=0.05):
    score = originality_score(submission1, submission2)
    return score > threshold

# this function is taken from scipy (ks_2samp) and modified and so falls
# under their BSD license
def originality_score(data1, data2):
    data2 = np.sort(data2)
    n1 = data1.shape[0]
    n2 = data2.shape[0]
    data_all = np.concatenate([data1, data2])
    cdf1 = np.searchsorted(data1, data_all, side='right') / (1.0*n1)
    cdf2 = np.searchsorted(data2, data_all, side='right') / (1.0*n2)
    d = np.max(np.absolute(cdf1 - cdf2))
    return d

def is_almost_unique(submission_data, submission, db_manager, filemanager, is_exact_dupe_thresh, is_similar_thresh, max_similar_models):
    num_similar_models = 0
    is_original = True
    similar_models = []
    is_not_a_constant = np.std(submission) > 0

    date_created = db_manager.get_date_created(submission_data['submission_id'])

    submission = np.sort(submission)
    for user_sub in db_manager.get_everyone_elses_recent_submssions(submission_data['competition_id'], submission_data['user'], date_created):
        with lock:
            other_submission = get_submission(db_manager, filemanager, user_sub["submission_id"])
        if other_submission is None:
            continue
        score = originality_score(submission, other_submission)

        if is_not_a_constant and np.std(other_submission) > 0 :
            correlation = pearsonr(submission, other_submission)[0]

            if np.abs(correlation) > 0.95:
                logging.getLogger().info("Found a highly correlated submission {} with score {}".format(user_sub["submission_id"], correlation))
                is_original = False
                break

        if score < is_exact_dupe_thresh:
            logging.getLogger().info("Found a duplicate submission {} with score {}".format(user_sub["submission_id"], score))
            is_original = False
            break
        if score <= is_similar_thresh:
            num_similar_models += 1
            similar_models.append(user_sub["submission_id"])
            if num_similar_models >= max_similar_models:
                logging.getLogger().info("Found too many similar models. Similar models were {}".format(similar_models))
                is_original = False
                break

    return is_original


def submission_originality(submission_data, db_manager, filemanager):
    """
    This checks a few things
        1. If the current submission is similar to the previous submission, we give it the same originality score
        2. Otherwise, we check that it is sufficently unique. To check this we see if it is A. Almost identitical to
        any other submission or B. Very similar to a handful of other models.

    """
    s = db_manager.db.submissions.find_one({'_id':ObjectId(submission_data['submission_id'])})
    submission_data['user'] = s['username']
    submission_data['competition_id'] = s['competition_id']
    logging.getLogger().info("Scoring {} {}".format(submission_data['user'], submission_data['submission_id']))

    with lock:
        submission = get_submission(db_manager, filemanager, submission_data['submission_id'])

    if submission is None:
        logging.getLogger().info("Couldn't find {} {}".format(submission_data['user'], submission_data['submission_id']))
        return

    is_exact_dupe_thresh = 0.005
    is_similar_thresh = 0.03
    max_similar_models = 1

    is_original = is_almost_unique(submission_data, submission, db_manager, filemanager, is_exact_dupe_thresh, is_similar_thresh, max_similar_models)
    db_manager.write_originality(submission_data['submission_id'], submission_data['competition_id'], is_original)
