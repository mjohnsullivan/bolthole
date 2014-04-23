"""
Tests store functionality
"""

import unittest
import os
import os.path
import shutil
import sys
import bolthole

DB_PATH = './test_db'

class TestDBMStore(unittest.TestCase):
    """
    anydbm store tests
    """

    def tearDown(self):
        """
        dbm data files are automatically appended with '.db'
        """
        if os.path.isfile(DB_PATH+'.db'):
            os.remove(DB_PATH+'.db')
        else:
            raise IOError('leveldb directory does not exist')

    def test_anydbm_create(self):
        db = bolthole.DBMStore(DB_PATH)
        db.close()
        self.assertTrue(os.path.exists(DB_PATH+'.db'))

    def test_anydbm_put_get(self):
        db = bolthole.DBMStore(DB_PATH)
        db.put('test_key', 'test_value')
        self.assertEqual(db.get('test_key'), 'test_value')
        db.close()


class TestLevelDBStore(unittest.TestCase):
    """
    leveldb store tests
    """

    def tearDown(self):
        """
        leveldb data files are stored in directory
        """
        if os.path.isdir(DB_PATH):
            shutil.rmtree(DB_PATH)
        else:
            raise IOError('leveldb directory does not exist')

    def test_leveldb_create(self):
        db = bolthole.LevelDBStore(DB_PATH)
        db.close()
        self.assertTrue(os.path.isdir(DB_PATH))

    def test_leveldb_put_get(self):
        db = bolthole.LevelDBStore(DB_PATH)
        db.put('test_key', 'test_value')
        self.assertEqual(db.get('test_key'), 'test_value')
        db.close()


class TestInMemoryStore(unittest.TestCase):
    """
    in-memory store tests
    """

    def test_leveldb_put_get(self):
        db = bolthole.InMemoryStore()
        db.put('test_key', 'test_value')
        self.assertEqual(db.get('test_key'), 'test_value')
        db.close()


class TestProcessStore(unittest.TestCase):
    """
    multiprocessing store tests
    """

    def test_process_create(self):
        db = bolthole.ProcessStore(None, 'mem')
        db.close()

    def test_process_put_get(self):
        db = bolthole.InMemoryStore()
        db.put('test_key', 'test_value')
        self.assertEqual(db.get('test_key'), 'test_value')
        db.close()
