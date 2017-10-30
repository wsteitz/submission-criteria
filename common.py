"""
Commonly used functions.
"""

# System
import os
from datetime import timedelta

# Third Party
import pandas as pd
from psycopg2 import connect
import pymongo
from bson.objectid import ObjectId
import boto3
import botocore
from sqlalchemy import create_engine
from sklearn.metrics import log_loss


def get_filename(db, submission_id):
    submission = db.submissions.find_one({"_id": ObjectId(submission_id)})
    fname = submission.get("filename", None)
    user = submission.get("username", None)
    if fname and user:
        return "{}/{}".format(user, fname), fname
    return None


def download_submission(db, submission_id):
    S3_BUCKET = os.environ.get("S3_UPLOAD_BUCKET", "numerai-production-uploads")
    S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
    s3 = boto3.resource("s3", aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)
    bucket = S3_BUCKET

    s3_file, filename = get_filename(db, submission_id)
    path = os.path.join("/tmp/", filename)
    if not os.path.isfile(path):
        try:
            s3.meta.client.download_file(bucket, s3_file, path)
        except botocore.exceptions.EndpointConnectionError:
            print("Could not download {} from S3. Skipping.".format(s3_file))
            return None
    return path


def connect_to_database():
    """Connect to the production Mongo database."""
    print("Using {} Mongo database credentials".format(os.environ.get("MONGO_CREDS")))
    client = pymongo.MongoClient(os.environ.get("MONGO_URL"))
    db = client[os.environ.get("MONGO_DB_NAME")]
    return db


def connect_to_postgres():
    """Connect to postgres database."""
    print("Using {} Postgres database credentials".format(os.environ.get("POSTGRES_CREDS")))
    return connect(os.environ.get("POSTGRES"))


def connect_to_public_targets_db():
    """Connect to the public targets database."""
    url = os.environ.get("SQL_URL")
    if url is None or url == "":
        raise Exception("You must specify SQL_URL")
    db = create_engine(url, echo=False)
    return db


def update_loglosses(submission_id, round_number):
    print("Updating loglosses...")
    # Get the submission
    db = connect_to_database()
    submission_path = download_submission(db, submission_id)
    submission = pd.read_csv(submission_path)
    mongo_submission = db.submissions.find_one({"_id": ObjectId(submission_id)})

    postgres_db = connect_to_postgres()
    cursor = postgres_db.cursor()
    cursor.execute("SELECT open_time FROM rounds WHERE number = {}".format(round_number))
    rounds = cursor.fetchall()
    round_open_time = rounds[0][0].date()
    round_data_date = round_open_time - timedelta(days=1)

    # Get the truth data
    public_targets_db = connect_to_public_targets_db()
    query = "SELECT id, target FROM tournament_historical_encrypted WHERE data_type = 'validation';"
    validation_data = pd.read_sql(query, public_targets_db)
    validation_data.sort_values("id", inplace=True)
    test_data = pd.read_sql("SELECT id, target FROM tournament_historical_encrypted WHERE data_type = 'test';", public_targets_db)
    test_data.sort_values("id", inplace=True)

    # Calculate logloss
    submission_validation_data = submission.loc[submission["id"].isin(validation_data["id"].as_matrix())].copy()
    submission_validation_data.sort_values("id", inplace=True)
    submission_test_data = submission.loc[submission["id"].isin(test_data["id"].as_matrix())].copy()
    submission_test_data.sort_values("id", inplace=True)
    validation_logloss = log_loss(validation_data["target"].as_matrix(), submission_validation_data["probability"].as_matrix())
    test_logloss = log_loss(test_data["target"].as_matrix(), submission_test_data["probability"].as_matrix())

    # Get the submission Postgres id
    query = "SELECT s.id FROM submissions s INNER JOIN users u ON s.user_id = u.id WHERE u.username = '{}' AND s.inserted_at = '{}'".format(mongo_submission["username"], mongo_submission["created"])
    cursor.execute(query)
    submission_id = cursor.fetchone()[0]

    query = "UPDATE submissions SET validation_logloss={}, test_logloss={} WHERE id = '{}'".format(validation_logloss, test_logloss, submission_id)
    cursor.execute(query)
    print("Updated {} with validation_logloss={} and test_logloss={}".format(submission_id, validation_logloss, test_logloss))
    postgres_db.commit()
    cursor.close()
    postgres_db.close()
