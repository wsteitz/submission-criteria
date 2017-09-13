Tournament submission criteria checks
=====================================

This server scores originality, concordance, and consistency.

To run the server locally: `./server.py --use_local`

Remove `--use_local` for production runs. This will spawn a sever on port 5151 (or set by ENV PORT), and listens for post requests that contain a form with user, `submission_id` and `competition_id`.

For example, the following python code works

`requests.post("http://localhost:5151/", json={'submission_id': '58d411e57278611200ee49a6', 'api_key': 'h/52y/E7cm8Ih4F3cVdlBM4ZQxER+Apk6P0L7yR0lFU='})`

To see the logic behind scoring originality and concordance, see those python files.

For production, the API sever requires the following environment variables

-   `MONGO_URL`
-   `MONGO_DB_NAME`
-   `S3_UPLOAD_BUCKET`
-   `S3_DATASET_BUCKET`
-   `S3_ACCESS_KEY`
-   `S3_SECRET_KEY`
-   `PORT`
-   `API_KEY`

Community
=========

For questions or discussion about the code, either file an issue or join our [Slack](https://slack.numer.ai/).

Bounty rules
============

What bounties?
--------------

See [this Medium post](https://medium.com/numerai/open-sourcing-model-evaluation-on-numerai-295c1ea3d001).

How will I receive my bounty?
-----------------------------

When you create the pull request, you must specify one or more usernames where the bounty will be sent. We will only send the bounty directly to user accounts. If you would like to hold your NMR somewhere else, you may withdraw it from your account.

Who will get the bounty if multiple people complete the same task?
------------------------------------------------------------------

If multiple people independently complete the same task, the first pull request that is approved will receive the bounty. We will accept the pull request that is code complete first, as determined by the timestamp of pull request or the *last* commit in the branch, whichever is later.

For example, suppose Alice and Bob are solving a task with a bounty, and Alice creates a pull request first, then Bob creates one. If Alice's pull request is approved with no changes, then she will receive the bounty. If, however, we request changes to Alice's PR, but Bob's is good to go, then Bob will receive the bounty. If both require changes, then whoever completes those changes first will get the bounty.

I promise I'm going to complete this task next week, can I claim it so that nobody finishes before me?
------------------------------------------------------------------------------------------------------

We do not allow users to claim tasks; bounties will be paid on a first-come, first-served basis. It may be advisable to let others know if you're working on an issue (for example, by commenting on the issue), but this is not required.

Since we may occasionally remove bounties if the changes are no longer necessary, you may wish to let us know you're working on the issue (publicly or privately). If you are actively working on an issue, we will generally not remove the bounty.

Finally, if you submit a pull request that we intend to approve, but we ask for a few small changes (e.g. formatting), we may explicitly temporarily "hold" the bounty for you, where if you complete the changes promptly, we will not give the bounty to anyone else.

What if I work together with someone to complete a task, and we would like to split the bounty?
-----------------------------------------------------------------------------------------------

Whoever makes the pull request needs to specify *in the pull request* how they would like the bounty split among users.

My pull request is awesome, but you guys won't approve it!
----------------------------------------------------------

Pull requests will be approved our Numerai's sole discretion. Bounties will be paid out only for approved pull requests.

I want to work on something, but there's no bounty for it.
----------------------------------------------------------

That's great, go ahead and work on it! Most potential improvements do not have bounties, but that doesn't mean they shouldn't be completed!

Can I make a pull request for these rules?
------------------------------------------

Yes.
