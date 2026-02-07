package com.zyc.magic_mirror.common.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RocksDBUtil {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBUtil.class);

    private static final long CACHE_EXPIRE_MINUTES = 5;

    private static final Cache<String, RocksDB> CONNECTION_CACHE;
    private static final Cache<String, RocksDB> READ_ONLY_CONNECTION_CACHE;

    static {
        RocksDB.loadLibrary();
        logger.info("RocksDB native library loaded successfully");

        CONNECTION_CACHE = CacheBuilder.newBuilder()
                .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
                .removalListener(new RocksDBRemovalListener())
                .build();

        READ_ONLY_CONNECTION_CACHE = CacheBuilder.newBuilder()
                .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
                .removalListener(new RocksDBRemovalListener())
                .build();

        logger.info("RocksDB连接缓存初始化完成，缓存过期时间: {} 分钟", CACHE_EXPIRE_MINUTES);
    }

    private static class RocksDBRemovalListener implements RemovalListener<String, RocksDB> {
        @Override
        public void onRemoval(RemovalNotification<String, RocksDB> notification) {
            if (notification.getValue() != null) {
                try {
                    notification.getValue().close();
                    logger.info("RocksDB连接已自动关闭 - 路径: {}, 原因: {}",
                            notification.getKey(), notification.getCause());
                } catch (Exception e) {
                    logger.error("自动关闭RocksDB连接失败 - 路径: {}", notification.getKey(), e);
                }
            }
        }
    }

    private RocksDBUtil() {
    }

    public static RocksDB getConnection(String path) throws RocksDBException {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("RocksDB路径不能为空");
        }

        try {
            return CONNECTION_CACHE.get(path, () -> {
                Options options = new Options()
                        .setCreateIfMissing(true)
                        .setCreateMissingColumnFamilies(true)
                        .setCompressionType(CompressionType.LZ4_COMPRESSION)
                        .setWriteBufferSize(64 * 1024 * 1024)
                        .setMaxWriteBufferNumber(3)
                        .setMaxBackgroundCompactions(4)
                        .setMaxBackgroundFlushes(2);

                RocksDB rocksDB = RocksDB.open(options, path);
                logger.info("RocksDB连接创建成功: {}", path);
                return rocksDB;
            });
        } catch (Exception e) {
            if (e instanceof RocksDBException) {
                throw (RocksDBException) e;
            }
            logger.error("获取RocksDB连接失败: {}", path, e);
            throw new RocksDBException("获取RocksDB连接失败: " + path);
        }
    }

    public static RocksDB getConnection(String path, Options options) throws RocksDBException {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("RocksDB路径不能为空");
        }
        if (options == null) {
            throw new IllegalArgumentException("Options不能为空");
        }

        try {
            return CONNECTION_CACHE.get(path, () -> {
                RocksDB rocksDB = RocksDB.open(options, path);
                logger.info("RocksDB连接创建成功（自定义Options）: {}", path);
                return rocksDB;
            });
        } catch (Exception e) {
            if (e instanceof RocksDBException) {
                throw (RocksDBException) e;
            }
            logger.error("获取RocksDB连接失败: {}", path, e);
            throw new RocksDBException("获取RocksDB连接失败: " + path);
        }
    }

    public static RocksDB getReadOnlyConnection(String path) throws RocksDBException {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("RocksDB路径不能为空");
        }

        try {
            return READ_ONLY_CONNECTION_CACHE.get(path, () -> {
                Options options = new Options()
                        .setCreateIfMissing(false)
                        .setCreateMissingColumnFamilies(false);

                RocksDB rocksDB = RocksDB.openReadOnly(options, path);
                logger.info("RocksDB只读连接创建成功: {}", path);
                return rocksDB;
            });
        } catch (Exception e) {
            if (e instanceof RocksDBException) {
                throw (RocksDBException) e;
            }
            logger.error("获取RocksDB只读连接失败: {}", path, e);
            throw new RocksDBException("获取RocksDB只读连接失败: " + path);
        }
    }

    public static void closeConnection(String path) {
        if (path == null || path.trim().isEmpty()) {
            logger.warn("关闭RocksDB连接失败: 路径为空");
            return;
        }

        RocksDB rocksDB = CONNECTION_CACHE.getIfPresent(path);
        if (rocksDB != null) {
            try {
                rocksDB.close();
                CONNECTION_CACHE.invalidate(path);
                logger.info("RocksDB连接已关闭: {}", path);
            } catch (Exception e) {
                logger.error("关闭RocksDB连接失败: {}", path, e);
            }
        }

        RocksDB readOnlyDb = READ_ONLY_CONNECTION_CACHE.getIfPresent(path);
        if (readOnlyDb != null) {
            try {
                readOnlyDb.close();
                READ_ONLY_CONNECTION_CACHE.invalidate(path);
                logger.info("RocksDB只读连接已关闭: {}", path);
            } catch (Exception e) {
                logger.error("关闭RocksDB只读连接失败: {}", path, e);
            }
        }
    }

    public static void closeAllConnections() {
        int closedCount = 0;

        for (String path : CONNECTION_CACHE.asMap().keySet()) {
            try {
                RocksDB rocksDB = CONNECTION_CACHE.getIfPresent(path);
                if (rocksDB != null) {
                    rocksDB.close();
                    logger.info("RocksDB连接已关闭: {}", path);
                    closedCount++;
                }
            } catch (Exception e) {
                logger.error("关闭RocksDB连接失败: {}", path, e);
            }
        }
        CONNECTION_CACHE.invalidateAll();

        for (String path : READ_ONLY_CONNECTION_CACHE.asMap().keySet()) {
            try {
                RocksDB rocksDB = READ_ONLY_CONNECTION_CACHE.getIfPresent(path);
                if (rocksDB != null) {
                    rocksDB.close();
                    logger.info("RocksDB只读连接已关闭: {}", path);
                    closedCount++;
                }
            } catch (Exception e) {
                logger.error("关闭RocksDB只读连接失败: {}", path, e);
            }
        }
        READ_ONLY_CONNECTION_CACHE.invalidateAll();

        logger.info("所有RocksDB连接已关闭，共关闭 {} 个连接", closedCount);
    }

    public static int getActiveConnectionCount() {
        return (int) (CONNECTION_CACHE.size() + READ_ONLY_CONNECTION_CACHE.size());
    }

    public static boolean isConnectionOpen(String path) {
        return CONNECTION_CACHE.getIfPresent(path) != null || READ_ONLY_CONNECTION_CACHE.getIfPresent(path) != null;
    }

    public static void put(String path, byte[] key, byte[] value) throws RocksDBException {
        RocksDB rocksDB = getConnection(path);
        rocksDB.put(key, value);
    }

    public static byte[] get(String path, byte[] key) throws RocksDBException {
        RocksDB rocksDB = getConnection(path);
        return rocksDB.get(key);
    }

    public static void delete(String path, byte[] key) throws RocksDBException {
        RocksDB rocksDB = getConnection(path);
        rocksDB.delete(key);
    }

    public static void put(String path, String key, String value) throws RocksDBException {
        RocksDB rocksDB = getConnection(path);
        rocksDB.put(key.getBytes(), value.getBytes());
    }

    public static String get(String path, String key) throws RocksDBException {
        RocksDB rocksDB = getConnection(path);
        byte[] value = rocksDB.get(key.getBytes());
        return value != null ? new String(value) : null;
    }

    public static void delete(String path, String key) throws RocksDBException {
        RocksDB rocksDB = getConnection(path);
        rocksDB.delete(key.getBytes());
    }

    public static long getCacheExpireMinutes() {
        return CACHE_EXPIRE_MINUTES;
    }
}
