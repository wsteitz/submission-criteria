import time
import statistics
import os

import randomstate as rnd
from multiprocessing import Pool
from originality import original

N_RUNS = 5000


def gen_submission(size=45000):
    return rnd.normal(loc=0.5, scale=0.1, size=(size,))


def check_original(_: int):
    submission_1, submission_2 = gen_submission(), gen_submission()
    t0 = time.time()
    original(submission_1, submission_2)
    t1 = time.time()
    return (t1 - t0) * 1000


pool_size = os.cpu_count() or 1
if pool_size > 1:
    pool_size = pool_size//2


with Pool(pool_size) as pool:
    times = pool.map(check_original, [i for i in range(N_RUNS)])

print('ran method %s times' % len(times))
print('median: %.2fms' % statistics.median(times))
print('mean: %.2fms' % statistics.mean(times))
print('stdev: %.2f' % statistics.stdev(times))
