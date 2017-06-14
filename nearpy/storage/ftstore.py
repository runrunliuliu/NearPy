import logging
import rocksdb
import cPickle as pickle


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
                self.store = self.readRocks(ind)

    def addBatch(self, kvs):
        batch = rocksdb.WriteBatch()
        for kv in kvs:
            batch.put(self.wrap(kv[0]), self.wrap(kv[1]))
        self.store.write(batch)

    def wrap(self, string):
        return string

    def add(self, key, val):
        if self.mode == 'MEM':
            self.store[key] = val
        if self.mode == 'ROCKS':
            self.store.put(self.wrap(key), self.wrap(val))

    def get(self, key):
        if self.mode == 'MEM':
            return self.store[key]
        if self.mode == 'ROCKS':
            logger.debug('Request key:{}'.format(key))
            vals = self.store.get(self.wrap(key))
            return pickle.loads(vals)
        
    def contains(self, key):
        ret = True
        if self.mode == 'MEM':
            if key not in self.store:
                ret = False
        if self.mode == 'ROCKS':
            ret = self.store.key_may_exist(key)[0]
        return ret

    def readRocks(self, ind):
        db = rocksdb.DB(self.dirs + '/' + ind + '.db', read_only=True)
        return db

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
