"""
Key/Value store process in Python. It will use either the anydbm or leveldb
storage engine depending on what's available on the system.

Hint: use leveldb
"""

import anydbm
try:
    import plyvel
except ImportError:
    print 'plyvel is not installed'

def create_store(db_path, kind=None):
    """
    Creates a new store; the kind of store can be specified,
    otherwise if plyvel is installed, then leveldb is used,
    if not, then it's anydbm.

    :param db_path: path to the physical storage - for leveldb this will be a directory;
        for anydbm this will be a file.
    :param kind: create a specific type of store - valid values are "dbm", "leveldb" and "mem"
    """
    if kind == 'anydbm':
        return DBMStore(db_path)
    elif kind == 'leveldb':
        import plyvel
        return LevelDBStore(db_path)
    elif kind == 'mem':
        return InMemoryStore()
    elif not kind:
        try:
            import plyvel
            return LevelDBStore(db_path)
        except ImportError:
            return DBMStore(db_path)
    else:
        raise TypeError('Unknown kind of store')


class Store(object):
    """
    General class for key value store
    """

    def get(self, key):
        """
        Returns a value associated with a key

        :param key: key value
        :returns: value associate with the key,
            or raises KeyError if the key does not exist
        :raises: KeyError
        """
        return self.db.get(key)

    def put(self, key, value):
        """
        Assigns value to key

        :param key: key value
        :param value: value assigned to key
        """
        self.db.put(key, value)

    def close(self):
        self.db.close()


class DBMStore(Store):
    """
    anydbm backed key value store
    """

    def __init__(self, db_path):
        self.db = anydbm.open(db_path, 'c')

    def put(self, key, value):
        self.db[key] = value


class LevelDBStore(Store):
    """
    leveldb backed key/value store
    """

    def __init__(self, db_path):
        self.db = plyvel.DB(db_path, create_if_missing=True)


class InMemoryStore(Store):
    """
    in-memory key/value store
    """

    def __init__(self):
        self.db = {}

    def put(self, key, value):
        self.db[key] = value

    def close(self):
        self.db = None


from multiprocessing import Manager, Process, Queue

class ProcessStore(Process):
    """
    Key/Value store that is run in a separate process; all
    interaction with the store is through a process-safe
    queue
    """

    def __init__(self, db_path=None, kind=None):
        super(ProcessStore, self).__init__(name='storage')
        self.queue = Queue()
        self.daemon = True # process dies when the parent dies
        self.db = create_store(db_path, kind)
        self.start() # start the process

    def run(self):
        while True:
            cmd, args, res = self.queue.get()
            if cmd == 'PUT':
                self.db.put(args[0], args[1])
            elif cmd == 'GET':
                res.put(self.db.get(args[0]))
            elif cmd == 'CLOSE':
                self.db.close()
                break

    def get(self, key):
        result_queue = Manager().Queue()
        self.queue.put(('GET', (key,), result_queue,))
        return result_queue.get()

    def put(self, key, value):
        self.queue.put(('PUT', (key, value,), None,))

    def close(self):
        self.queue.put(('CLOSE', None, None,))


if __name__ == '__main__':
    p = ProcessStore(kind='mem')
    import time
    time.sleep(60)