import logging
import rocksdb
import redis
try:
        import cPickle as pickle
except:
        import pickle
import numpy as np
import zlib


logger  = logging.getLogger('FTStore') 


class FTStore(object):

    def __init__(self, mode='MEM', ind='test', load=True):
        self.dirs  = '0_DB'
        self.store = None
        self.mode  = mode

        if mode == 'MEM':
            self.store = dict()
        if mode == 'ROCKS':
            if load is True:
                self.store = self.initRocks(ind)
            else:
                self.store = self.initRocks(ind)
        if mode == 'REDIS':
            self.store = self.initRedis(ind)

    def addBatch(self, kvs):
        batch = rocksdb.WriteBatch()
        for kv in kvs:
            batch.put(self.wrap(kv[0]), self.wrap(kv[1]))
        self.store.write(batch, disable_wal=True)

    def wrap(self, string):
        return string

    def delete(self):
        del self.store
        self.store = None
        logger.info('DEL_STORE_SUCCESS')

    def update(self, store):
        if self.mode == 'MEM':
            self.store.update(store)
        if self.mode == 'ROCKS':
            self.store = store

    def add(self, key, val):
        if self.mode == 'MEM':
            val0 = np.asarray(val[0] * 10000000, int)
            cz   = zlib.compress(val0)
            self.store[key] = (cz, val[1])
        if self.mode == 'ROCKS':
            self.store.put(self.wrap(key), self.wrap(val), disable_wal=True)
        if self.mode == 'REDIS':
            val0 = np.asarray(val[0] * 10000000, int)
            cz   = zlib.compress(val0)
            self.store.set(key, cz)

    def get(self, key):

        if self.mode == 'MEM':
            val  = self.store[key]
            dstr = zlib.decompress(val[0])
            dval = np.fromstring(dstr, dtype=int)
            dval = dval / 10000000.0 
            del dstr
            dstr = None
            return (dval, val[1])

        if self.mode == 'ROCKS':
            logger.debug('Request key:{}'.format(key))
            vals = self.store.get(self.wrap(key))
            if vals is None:
                return None
            return pickle.loads(vals)

        if self.mode == 'REDIS':
            val  = self.store.get(key)
            dstr = zlib.decompress(val[0])
            dval = np.fromstring(dstr, dtype=int)
            dval = dval / 10000000.0 
            del dstr
            dstr = None
            return (dval, val[1])

        
    def contains(self, key):
        ret = True
        if self.mode == 'MEM':
            if key not in self.store:
                ret = False
        if self.mode == 'ROCKS':
            ret = self.store.key_may_exist(key)[0]
        if self.mode == 'REDIS':
            ret = r.exists(key)
        return ret

    def readRocks(self, ind):
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.max_open_files = 300000
        opts.write_buffer_size = 1073741824
        opts.max_write_buffer_number = 20
        opts.target_file_size_base = 67108864
        opts.max_background_compactions = 8
        opts.max_background_flushes = 4

        opts.table_factory = rocksdb.BlockBasedTableFactory(
            filter_policy=rocksdb.BloomFilterPolicy(10),
            block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
            block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))

        db = rocksdb.DB(self.dirs + '/' + ind + '.db', opts, read_only=True)
        return db

    def initRedis(self, ind):
        port = 0
        if ind == 'k3':
            port = 28898
        r = redis.StrictRedis(host='localhost', port=port, db=0)
        return r

    def initRocks(self, ind):
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.max_open_files = 300000
        opts.write_buffer_size = 1073741824
        opts.max_write_buffer_number = 20
        opts.target_file_size_base = 67108864
        opts.max_background_compactions = 8
        opts.max_background_flushes = 4

        opts.table_factory = rocksdb.BlockBasedTableFactory(
            filter_policy=rocksdb.BloomFilterPolicy(10),
            block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
            block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))

        db = rocksdb.DB(self.dirs + '/' + ind + '.db', opts)
        return db
#
