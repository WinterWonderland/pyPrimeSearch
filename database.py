import sqlite3
from functools import wraps


def retry_on_database_locked(func):
    @wraps(func)
    def retry_on_database_locked_wrapper(*args, **kwargs):
        success = False
        while not success:
            try:
                return_value = func(*args, **kwargs)
            except sqlite3.Error as exception:
                success = False
            else:
                success = True
        return return_value
    return retry_on_database_locked_wrapper


class PrimeDatabase:
    def __init__(self, path):
        self._path = path
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS AllPrimes(Value INT)")
            cursor.execute("SELECT COUNT(Value) FROM AllPrimes")
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO AllPrimes VALUES(2)")
        
    @retry_on_database_locked
    def get_prime_count(self):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            cursor.execute("SELECT COUNT(Value) FROM AllPrimes")
            return cursor.fetchone()[0]

    @retry_on_database_locked
    def save_new_prime(self, value):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            cursor.execute("INSERT INTO AllPrimes VALUES(" + str(value) + ")")

    @retry_on_database_locked
    def save_prime_list(self, prime_list):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            for prime in prime_list:
                cursor.execute("INSERT INTO AllPrimes VALUES(" + str(prime) + ")")

    @retry_on_database_locked
    def get_all_primes(self):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            cursor.execute("SELECT Value FROM AllPrimes")
            return list([value[0] for value in cursor.fetchall()])

    @retry_on_database_locked
    def get_max_prime(self):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            cursor.execute("SELECT MAX(Value) FROM AllPrimes")
            return cursor.fetchone()[0]
        
    @retry_on_database_locked
    def check_prime(self, value):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            cursor.execute("SELECT 1 FROM AllPrimes WHERE Value=" + str(value))
            try:
                cursor.fetchone()[0]
                is_prime = True
            except:
                is_prime = False
            return is_prime
                