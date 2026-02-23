# test_mp.py
import os
import time
from multiprocessing import Pool

def work(i):
    print(f"Worker {i}, PID={os.getpid()}")
    time.sleep(2)
    return i

if __name__ == "__main__":
    from multiprocessing import set_start_method
    set_start_method("spawn")

    n = 15
    with Pool(n) as p:
        p.map(work, range(n))