import sqlite3

class PrimeDatabase:
    def __init__(self, path):
        self._path = path
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            sucess = False
            while not sucess:
                try:
                    cursor.execute("CREATE TABLE IF NOT EXISTS AllPrimes(Value INT)")
                    cursor.execute("SELECT COUNT(Value) FROM AllPrimes")
                    sucess = True
                except sqlite3.OperationalError:
                    pass
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO AllPrimes VALUES(2)")
        
    def get_prime_count(self):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            sucess = False
            while not sucess:
                try:
                    cursor.execute("SELECT COUNT(Value) FROM AllPrimes")
                    sucess = True
                except sqlite3.OperationalError:
                    pass
            return cursor.fetchone()[0]

    def save_new_prime(self, value):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            sucess = False
            while not sucess:
                try:
                    cursor.execute("INSERT INTO AllPrimes VALUES(" + str(value) + ")")
                    sucess = True
                except sqlite3.OperationalError:
                    pass
                    

    def save_prime_list(self, prime_list):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            for prime in prime_list:
                sucess = False
                while not sucess:
                    try:
                        cursor.execute("INSERT INTO AllPrimes VALUES(" + str(prime) + ")")
                        sucess = True
                    except sqlite3.OperationalError:
                        pass

    def get_all_primes(self):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            sucess = False
            while not sucess:
                try:
                    cursor.execute("SELECT Value FROM AllPrimes")
                    sucess = True
                except sqlite3.OperationalError:
                    pass
            return list([value[0] for value in cursor.fetchall()])

    def get_max_prime(self):
        with sqlite3.connect(self._path) as database_connection:
            cursor = database_connection.cursor()
            sucess = False
            while not sucess:
                try:
                    cursor.execute("SELECT MAX(Value) FROM AllPrimes")
                    sucess = True
                except sqlite3.OperationalError:
                    pass
            return cursor.fetchone()[0]
