import math
import time
import multiprocessing
import database
import psutil
          
                
class PrimeFeeder(multiprocessing.Process):
    def __init__(self, stop_flag, job_queue, start_value, block_size):
        self.stop_flag = stop_flag
        self.job_queue = job_queue
        self.start_value = start_value
        self.block_size = block_size
        super().__init__()
    
    def run(self):
        while not self.stop_flag.value:
            try:
                self.job_queue.put((self.start_value, self.start_value + self.block_size.value), block=True, timeout=1.0)
                self.start_value += self.block_size.value
            except:
                pass               

                
class PrimeWorker(multiprocessing.Process):
    def __init__(self, stop_flag, job_queue, prime_queue):
        self.stop_flag = stop_flag
        self.job_queue = job_queue
        self.prime_queue = prime_queue
        self.block_times = multiprocessing.Queue(maxsize=10)
        super().__init__()
        
    def run(self):
        while not self.stop_flag.value:
            try:
                start_time = time.perf_counter()
                
                start_value, end_value = self.job_queue.get(timeout=1.0)
                for test_value in range(start_value, end_value):
                    if self.check_prime(test_value):
                        self.prime_queue.put(test_value)
                self.job_queue.task_done()
                
                end_time = time.perf_counter()
                if self.block_times.full():
                    self.block_times.get()
                self.block_times.put(end_time - start_time)
            except:
                pass
                
    def check_prime(self, test_value):
        if test_value % 2 == 0 and test_value > 2:
            return False
        return all(test_value % i for i in range(3, int(math.sqrt(test_value)) + 1, 2))

    def get_block_times(self):
        block_times = []
        while not self.block_times.empty():
            block_times.append(self.block_times.get())
        return block_times
        
  
class PrimeSaver(multiprocessing.Process):
    def __init__(self, stop_flag, prime_queue, primes_database):
        self.stop_flag = stop_flag
        self.prime_queue = prime_queue
        self.primes_database = primes_database
        super().__init__()
        
    def run(self):
        while not self.stop_flag.value:
            values = []
            while not self.prime_queue.empty():
                values.append(self.prime_queue.get())
            self.primes_database.save_prime_list(values)
            for value in values:
                self.prime_queue.task_done()
            time.sleep(0.1)


if __name__ == "__main__":  
    primes_database = database.PrimeDatabase(r"..\Data\Primes.sqlite")
    start_value = primes_database.get_max_prime() + 1
    
    worker_count = multiprocessing.cpu_count()
    job_queue = multiprocessing.JoinableQueue(maxsize=worker_count * 2)
    prime_queue = multiprocessing.JoinableQueue()
    
    
    stop_flag_feeder = multiprocessing.Value("b", False)
    block_size_feeder = multiprocessing.Value("i", 1)
    feeder = PrimeFeeder(stop_flag_feeder, 
                         job_queue,
                         start_value,
                         block_size_feeder)
    feeder.daemon = True
    feeder.start()
    psutil.Process(pid=feeder.pid).nice(psutil.HIGH_PRIORITY_CLASS)
    
    
    stop_flag_worker = multiprocessing.Value("b", False)
    workers = []
    for _ in range(worker_count):
        worker = PrimeWorker(stop_flag_worker, 
                             job_queue,
                             prime_queue)
        worker.daemon = True
        worker.start()
        workers.append(worker)
    
    
    stop_flag_saver = multiprocessing.Value("b", False)
    saver = PrimeSaver(stop_flag_saver, 
                       prime_queue,
                       primes_database)
    saver.daemon = True
    saver.start()
    psutil.Process(pid=saver.pid).nice(psutil.HIGH_PRIORITY_CLASS)
    
    
    for _ in range(1500):
        time.sleep(10)
        print("")
        block_size = feeder.block_size.value
        print("block size", block_size)
        
        block_times = []
        for worker in workers:
            block_times.extend(worker.get_block_times())
            
        if block_times:
            mean_time = sum(block_times) / len(block_times)
            print("mean block time", mean_time)
            if mean_time > 0:
                prefered_block_time = 1.0
                damping = 0.7
                feeder.block_size.value = max(1, int(block_size * (1 + ((prefered_block_time / mean_time - 1) * damping))))
                
        print("prime count", primes_database.get_prime_count())
        print("max prime", primes_database.get_max_prime())

        
    stop_flag_feeder.value = True
    feeder.join()
    
    job_queue.join()
    stop_flag_worker.value = True
    for worker in workers:
        worker.join()
        
    prime_queue.join()
    stop_flag_saver.value = True
    saver.join()
    