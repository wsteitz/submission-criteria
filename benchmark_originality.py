import time
import statistics
import os

import randomstate as rnd
from multiprocessing import Pool
from originality import original

N_RUNS = 50


def gen_submission(predictions=45000, users=1000):
    # numpy's RandomSeed don't play well with multiprocessing; randomstate is a drop-in replacement
    return rnd.normal(loc=0.5, scale=0.1, size=(predictions, users))


def check_original(new_submission, other_submissions):
    t0 = time.time()
    n_predictions = new_submission.shape[0]
    for i in range(other_submissions.shape[1]):
        original(new_submission.reshape(n_predictions,), other_submissions[:, i])
    t1 = time.time()
    return (t1 - t0) * 1000


def run_benchmark():
    # try to use half the available cores to avoid shaky medians per run caused by cpu usage from other processes
    pool_size = os.cpu_count() or 1
    if pool_size > 1:
        pool_size = pool_size//2

    new_submission = gen_submission(users=1)
    other_submissions = gen_submission(users=1000)

    with Pool(pool_size) as pool:
        times = pool.starmap(check_original, [(new_submission, other_submissions) for _ in range(N_RUNS)])

    print('ran method %s times' % len(times))
    print('median: %.2fms' % statistics.median(times))
    print('mean: %.2fms' % statistics.mean(times))
    print('stdev: %.2f' % statistics.stdev(times))


if __name__ == '__main__':
    run_benchmark()
