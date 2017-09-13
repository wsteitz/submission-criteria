# System
"""MongoDB Data Access Object."""
import os
import datetime
import logging
import math

# Third Party
import pymongo as pymongo
from bson.objectid import ObjectId
import pandas as pd
from sklearn.metrics import log_loss
import numpy as np

# First Party
from concordance import get_submission_pieces

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017/numerai-dev")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "numerai-dev")

def connect_db(local_db = True):
    """Make a connection to either the production or local database. The defintions
    of local and production can be changed with environment variables.
    """

    url = MONGO_URL
    db_name = MONGO_DB_NAME

    client = pymongo.MongoClient(url)
    return client[db_name]

class DatabaseManager(object):

    def __init__(self, local_db = True):
        self.db = connect_db(local_db)

    def __hash__(self):
        """
        We want to implement the hash function so we can use this with a lru_cache
        but we don't actually care about hashing it.
        """
        return 314159

    def update_leaderboard(self, submission_id, filemanager):
        """Update the leaderboard with a submission

        Parameters:
        ----------
        submission_id : string
            ID of the submission

        filemanager : FileManager
            S3 Bucket data access object for querying competition datasets
        """
        submission = self.db.submissions.find_one({"_id":ObjectId(submission_id)})
        submission_id = submission["_id"]
        competition_id = submission["competition_id"]

        # Get the tournament data
        extract_dir = filemanager.download_dataset(competition_id)
        tournament_data = pd.read_csv(os.path.join(extract_dir, "numerai_tournament_data.csv"))
        # Get the user submission
        s3_file = self.get_filename(submission_id)
        local_file = filemanager.download([s3_file])[0]
        submission_data = pd.read_csv(local_file)
        validation_data = tournament_data[tournament_data.data_type == "validation"]
        validation_submission_data = submission_data[submission_data.id.isin(validation_data.id.values)]
        validation_eras = np.unique(validation_data.era.values)
        num_eras = len(validation_eras)

        # Calculate era loglosses
        better_than_random_era_count = 0

        for era in validation_eras:
            era_data = validation_data[validation_data.era == era]
            submission_era_data = validation_submission_data[validation_submission_data.id.isin(era_data.id.values)]
            era_data = era_data.sort_values(["id"])
            submission_era_data = submission_era_data.sort_values(["id"])
            logloss = log_loss(era_data.target.values, submission_era_data.probability.values)
            if logloss < -math.log(0.5):
                better_than_random_era_count += 1

        consistency = better_than_random_era_count / num_eras * 100

        print("Consistency: {}".format(consistency))

        self.db.submissions.update_one(
            {"_id": submission_id},

            {
                "$set": {
                    "consistency": consistency,
                    "concordant": {
                        "pending": True
                    },
                    "original": {
                        "pending": True
                    }
                }
            }
        )

        competition_id = submission["competition_id"]

        # TODO remove list comprehension, change to find_one, and change checks/loops
        lb_position = [a for a in self.db.competitions.find({"_id": int(competition_id)}, {"leaderboard": {"$elemMatch": {"username": submission["username"]}}})]

        is_in_leaderboard = False
        for lb in lb_position:
            try:
                lb["leaderboard"]
                is_in_leaderboard = True
            except:
                pass

        if is_in_leaderboard:

            self.db.competitions.update_one(
                {
                    "_id": int(competition_id),
                    "leaderboard": {
                        "$elemMatch": {
                            "username": submission["username"]
                        }
                    }
                },

                {
                    "$set": {
                        "leaderboard.$.submission_id": ObjectId(submission_id),
                        "leaderboard.$.logloss.validation": submission["validationLogloss"],
                        "leaderboard.$.logloss.consistency": consistency,
                        "leaderboard.$.concordant": {
                            "pending": True
                        },

                        "leaderboard.$.original": {
                            "pending": True
                        }
                    }
                },

                upsert=False
            )

        else:
            user = self.db.users.find_one({"username":submission["username"]})
            self.db.competitions.update(
                {"_id": int(competition_id)},

                {
                    "$push": {
                        "leaderboard": {
                            "username": submission["username"],

                            "submission_id": ObjectId(submission_id),

                            "earnings": {
                                "career": {
                                    "usd": user["earnings"]["career"]["usd"],
                                    "nmr": user["earnings"]["career"]["nmr"]
                                },

                                "competition": {
                                    "usd": 0,
                                    "nmr": 0
                                }
                            },

                            "logloss": {
                                "validation": submission["validationLogloss"],
                                "consistency": consistency
                            },

                            "concordant": {
                                "pending": True
                            },

                            "original": {
                                "pending": True
                            }
                        }
                    }
                }
            )

    def write_concordance(self, submission_id, competition_id, concordance):
        """Write to both the submission and leaderboard

        Parameters:
        -----------
        submission_id : string
            ID of the submission

        competition_id : int
            The numerical ID of the competition round

        concordance : bool
            The calculated concordance for a submission
        """
        concordance = bool(concordance)
        logging.getLogger().info("Writing out submission_id {} concordance {}".format(submission_id, concordance))

        self.db.submissions.update_one(
            {"_id": ObjectId(submission_id)},

            {
                "$set": {
                    "concordant": {
                        "pending": False,
                        "value": concordance
                    }
                }
            },

            upsert=False
        )

        lb_position = [a for a in self.db.competitions.find({"_id": int(competition_id)}, {"leaderboard": {"$elemMatch": {"submission_id": ObjectId(submission_id)}}})]

        if len(lb_position)>0:
            self.db.competitions.update_one(
                {
                    "_id": int(competition_id),
                    "leaderboard": {
                        "$elemMatch": {
                            "submission_id": ObjectId(submission_id)
                        }
                    }
                },

                {
                    "$set": {
                        "leaderboard.$.concordant": {
                            "pending": False,
                            "value": concordance
                        }
                    }
                },

                upsert=False
            )


    def write_originality(self, submission_id, competition_id, is_original):
        """ Write to both the submission and leaderboard

        Parameters:
        -----------
        submission_id : string
            The ID of the submission

        competition_id : int
            The numerical ID of the competition round

        is_original : bool
            Originality value for the submission
        """
        #TODO: change to reference submission data directly in leaderboard (instead of duplicating data manually)
        logging.getLogger().info("Writing out submission_id {}  originality {}".format(submission_id, is_original))

        self.db.submissions.update_one(
            {"_id": ObjectId(submission_id)},

            {
                "$set": {
                    "original": {
                        "pending": False,
                        "value": is_original
                    }
                }
            },

            upsert=False
        )

        lb_position = [a for a in self.db.competitions.find({"_id": int(competition_id)}, {"leaderboard": {"$elemMatch": {"submission_id": ObjectId(submission_id)}}})]

        if len(lb_position)>0:
            self.db.competitions.update_one(
                {
                    "_id": int(competition_id),
                    "leaderboard": {
                        "$elemMatch": {
                            "submission_id": ObjectId(submission_id)
                        }
                    }
                },

                {
                    "$set": {
                        "leaderboard.$.original": {
                            "pending": False,
                            "value": is_original
                        }
                    }
                },

                upsert=False
            )

    def get_originality(self, submission_id):
        """Get the originality for a submission_id

        Parameters:
        -----------
        submission_id : string
            The ID of the submission

        Returns:
        --------
        bool
            Whether the submission was deemed original
        """
        submission = self.db.submissions.find_one({"_id":ObjectId(submission_id)})
        if "original" in submission:
            return submission["original"]
        return True

    def get_everyone_elses_recent_submssions(self, competition_id, username, end_time = None):
        """ Get all the submissions, excluding those by username, up to time end_time.

        Parameters:
        -----------
        competition_id : int
            The numerical ID of the competition round

        username : string
            The username belonging to the submission

        endtime : time, optional, default: None
            Lookback window for querying recent submissions

        Returns:
        --------
        submissions : list
            List of all recent submissions for the competition round less than end_time
        """

        if end_time is None:
            end_time = datetime.datetime.utcnow()

        pipeline = [{
            "$match": {
                "competition_id": int(competition_id),
                "created": {
                    "$lt": end_time
                }
            }
        },

        {
            "$sort": {
                "created": -1
            }
        },

        {
            "$group": {
                "_id": "$username",
                "username": {
                    "$first": "$username"
                },
                "filename": {
                    "$first": "$filename"
                },
                "submission_id": {
                    "$first": "$_id"
                },
                "created": {
                    "$first": "$created"
                }
            }
        }]

        submissions = []

        for submission in self.db.submissions.aggregate(pipeline):
            if submission["username"] == username:
                continue
            submissions.append(submission)
        return submissions


    def get_filename(self, submission_id):
        """Get the filename that is used by S3 based on submission_id

        Paramters:
        ----------
        submission_id:
            The ID of the submission_id

        Returns:
        --------
        string
            The filename belonging to the submission id, if not found return None
        """
        submission = self.db.submissions.find_one({"_id": ObjectId(submission_id)})
        fname = submission.get("filename", None)
        user = submission.get("username", None)
        if fname and user:
            return "{}/{}".format(user, fname)
        else:
            return None

    def get_date_created(self, submission_id):
        """Get the date create for a submission"""
        submission = self.db.submissions.find_one({"_id":ObjectId(submission_id)})
        return submission["created"]
