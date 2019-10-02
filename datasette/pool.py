import contextlib
import threading

from datasette.utils import sqlite3


class ConnectionError(Exception):
    pass


class ConnectionTimeoutError(ConnectionError):
    pass


class Connection:
    def __init__(self, name, connect_args=None, connect_kwargs=None):
        self.name = name
        self.connect_args = connect_args or tuple()
        self.connect_kwargs = connect_kwargs or {}
        self.lock = threading.Lock()
        self._connection = None

    def connection(self):
        if self._connection is None:
            self._connection = sqlite3.connect(
                *self.connect_args, **self.connect_kwargs
            )
        return self._connection

    def __repr__(self):
        return "{} {} ({})".format(self.name, self._connection, self.lock)


class ConnectionGroup:
    timeout = 2

    def __init__(self, name, connect_args, connect_kwargs=None, limit=3):
        self.name = name
        self.connections = [
            Connection(name, connect_args, connect_kwargs) for _ in range(limit)
        ]
        self.limit = limit
        self._semaphore = threading.Semaphore(value=limit)

    @contextlib.contextmanager
    def connection(self):
        semaphore_aquired = False
        reserved_connection = None
        try:
            semaphore_aquired = self._semaphore.acquire(timeout=self.timeout)
            if not semaphore_aquired:
                raise ConnectionTimeoutError(
                    "Timed out after {}s waiting for connection '{}'".format(
                        self.timeout, self.name
                    )
                )
            # Loop through connections attempting to aquire a lock
            for connection in self.connections:
                lock = connection.lock
                if lock.acquire(False):
                    # We acquired the lock! use this one
                    reserved_connection = connection
                    break
            else:
                # If we get here, we failed to lock a connection even though
                # the semaphore should have guaranteed it
                raise ConnectionError(
                    "Failed to lock a connection despite the sempahore"
                )
            # We should have a connection now - yield it, then clean up locks
            yield reserved_connection
        finally:
            reserved_connection.lock.release()
            if semaphore_aquired:
                self._semaphore.release()


class Pool:
    def __init__(self, databases=None, max_connections_per_database=3):
        self.max_connections_per_database = max_connections_per_database
        self.databases = {}
        self.connection_groups = {}
        for key, value in (databases or {}).items():
            self.add_database(key, value)

    def add_database(self, name, filepath):
        self.databases[name] = filepath
        self.connection_groups[name] = ConnectionGroup(
            name, [filepath], limit=self.max_connections_per_database
        )

    @contextlib.contextmanager
    def connection(self, name):
        with self.connection_groups[name].connection() as conn:
            yield conn


# pool = Pool(
#     {
#         "trees": "/Users/simonw/Dropbox/Development/sf-trees.db",
#         "healthkit": "/Users/simonw/Dropbox/dogsheep/healthkit.db",
#     }
# )

# with pool.connection("fixtures") as conn:
#     conn.set_time_limit(1000)
#     conn.allow_all()
#     conn.execute(...)
