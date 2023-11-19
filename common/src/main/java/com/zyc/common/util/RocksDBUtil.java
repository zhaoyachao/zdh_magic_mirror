package com.zyc.common.util;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBUtil {

    static {
        RocksDB.loadLibrary();
    }

    public synchronized static RocksDB getConnection(String path) throws RocksDBException {
        Options options = new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        RocksDB rocksDB = RocksDB.open(options, path);
        return rocksDB;
    }

    public synchronized static RocksDB getReadOnlyConnection(String path) throws RocksDBException {
        Options options = new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        RocksDB rocksDB = RocksDB.openReadOnly(path);
        return rocksDB;
    }
}
