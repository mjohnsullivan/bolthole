Store
=====

Creating a Store
----------------

The system will dynamically choose the storage engine based on what's
installed: leveldb or defaults to anydbm

To create a store, use:

.. autofunction:: bolthole.create_store

A store has:

.. autoclass:: bolthole.Store
   :members: get, put, close
