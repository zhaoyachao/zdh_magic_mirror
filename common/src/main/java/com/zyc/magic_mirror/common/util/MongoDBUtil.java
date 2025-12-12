package com.zyc.magic_mirror.common.util;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * MongoDB工具类，封装常用操作
 */
public class MongoDBUtil {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBUtil.class);

    private final MongoClient mongoClient;

    private final String dbName;

    /**
     * 私有构造函数，初始化连接池
     * @param connectionString 连接字符串
     * @param dbName 数据库
     * @param maxPoolSize 最大连接数
     * @param minPoolSize 最小连接数
     * @param maxWaitTime 最大等待时间(秒)
     */
    private MongoDBUtil(String connectionString,String dbName,
                       int maxPoolSize, int minPoolSize, int maxWaitTime) {
        try {
            // 配置连接字符串
            ConnectionString connString = new ConnectionString(connectionString);
            
            // 配置MongoDB客户端设置
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(connString)
                    .applyToConnectionPoolSettings(builder -> 
                        builder.maxSize(maxPoolSize)
                               .minSize(minPoolSize)
                               .maxWaitTime(maxWaitTime, TimeUnit.SECONDS)
                               .maxConnectionLifeTime(0, TimeUnit.MILLISECONDS)
                               .maintenanceFrequency(1, TimeUnit.MINUTES)
                    )
                    .applyToSocketSettings(builder -> 
                        builder.connectTimeout(10, TimeUnit.SECONDS)
                               .readTimeout(0, TimeUnit.SECONDS)
                    )
                    .build();
            
            // 创建MongoClient实例
            this.mongoClient = MongoClients.create(settings);
            this.dbName = dbName;
            logger.info("MongoDB连接池初始化成功");
        } catch (Exception e) {
            logger.error("MongoDB连接池初始化失败", e);
            throw new RuntimeException("MongoDB连接池初始化失败", e);
        }
    }
    

    /**
     * 自定义配置获取单例实例
     * @param connectionString 连接字符串
     * @param dbName 数据库
     * @param maxPoolSize 最大连接数
     * @param minPoolSize 最小连接数
     * @param maxWaitTime 最大等待时间(秒)
     * @return MongoDBUtil实例
     */
    public static MongoDBUtil getInstance(String connectionString,String dbName,
                                         int maxPoolSize, int minPoolSize, int maxWaitTime) {
        return new MongoDBUtil(
                connectionString,
                dbName,
                maxPoolSize,
                minPoolSize,
                maxWaitTime
        );
    }
    
    /**
     * 获取数据库
     * @return MongoDatabase实例
     */
    public MongoDatabase getDatabase() {
        return mongoClient.getDatabase(dbName);
    }
    
    /**
     * 获取集合
     * @param collectionName 集合名
     * @return MongoCollection<Document>实例
     */
    public MongoCollection<Document> getCollection(String collectionName) {
        return getDatabase().getCollection(collectionName);
    }


    /**
     * 获取集合
     * @param collectionName 集合名
     * @return MongoCollection<T>实例
     */
    public <T> MongoCollection<T> getCollection(String collectionName, Class<T> cls) {
        return getDatabase().getCollection(collectionName, cls);
    }

    // -------------------------- 插入操作 --------------------------
    
    /**
     * 插入单个文档
     * @param collectionName 集合名
     * @param document 文档
     * @return 插入结果
     */
    public void insertOne(String collectionName, Document document) {
        try {
            getCollection(collectionName).insertOne(document);
        } catch (Exception e) {
            logger.error("插入文档失败", e);
            throw e;
        }
    }
    
    /**
     * 批量插入文档
     * @param collectionName 集合名
     * @param documents 文档列表
     * @return 插入结果
     */
    public void insertMany(String collectionName, List<Document> documents) {
        try {
            getCollection(collectionName).insertMany(documents);
        } catch (Exception e) {
            logger.error("批量插入文档失败", e);
            throw e;
        }
    }
    
    // -------------------------- 查询操作 --------------------------
    
    /**
     * 根据条件查询单个文档
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @return 文档
     */
    public Document findOne(String collectionName, Bson filter) {
        try {
            return getCollection(collectionName).find(filter).first();
        } catch (Exception e) {
            logger.error("查询单个文档失败", e);
            throw e;
        }
    }
    
    /**
     * 根据条件查询多个文档
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @return 文档迭代器
     */
    public FindIterable<Document> find(String collectionName, Bson filter) {
        try {
            return getCollection(collectionName).find(filter);
        } catch (Exception e) {
            logger.error("查询多个文档失败", e);
            throw e;
        }
    }
    
    /**
     * 查询集合中所有文档
     * @param collectionName 集合名
     * @return 文档迭代器
     */
    public FindIterable<Document> findAll(String collectionName) {
        return find(collectionName, new Document());
    }
    
    /**
     * 统计文档数量
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @return 文档数量
     */
    public long count(String collectionName, Bson filter) {
        try {
            return getCollection(collectionName).countDocuments(filter);
        } catch (Exception e) {
            logger.error("统计文档数量失败", e);
            throw e;
        }
    }
    
    // -------------------------- 更新操作 --------------------------
    
    /**
     * 更新单个文档
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @param update 更新操作
     * @return 更新结果
     */
    public UpdateResult updateOne(String collectionName, Bson filter, Bson update) {
        return updateOne(collectionName, filter, update, new UpdateOptions());
    }
    
    /**
     * 更新单个文档（带更新选项）
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @param update 更新操作
     * @param options 更新选项
     * @return 更新结果
     */
    public UpdateResult updateOne(String collectionName, Bson filter, Bson update, UpdateOptions options) {
        try {
            return getCollection(collectionName).updateOne(filter, update, options);
        } catch (Exception e) {
            logger.error("更新单个文档失败", e);
            throw e;
        }
    }
    
    /**
     * 更新多个文档
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @param update 更新操作
     * @return 更新结果
     */
    public UpdateResult updateMany(String collectionName, Bson filter, Bson update) {
        return updateMany(collectionName, filter, update, new UpdateOptions());
    }
    
    /**
     * 更新多个文档（带更新选项）
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @param update 更新操作
     * @param options 更新选项
     * @return 更新结果
     */
    public UpdateResult updateMany(String collectionName, Bson filter, Bson update, UpdateOptions options) {
        try {
            return getCollection(collectionName).updateMany(filter, update, options);
        } catch (Exception e) {
            logger.error("更新多个文档失败", e);
            throw e;
        }
    }
    
    // -------------------------- 删除操作 --------------------------
    
    /**
     * 删除单个文档
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @return 删除结果
     */
    public DeleteResult deleteOne(String collectionName, Bson filter) {
        return deleteOne(collectionName, filter, new DeleteOptions());
    }
    
    /**
     * 删除单个文档（带删除选项）
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @param options 删除选项
     * @return 删除结果
     */
    public DeleteResult deleteOne(String collectionName, Bson filter, DeleteOptions options) {
        try {
            return getCollection(collectionName).deleteOne(filter, options);
        } catch (Exception e) {
            logger.error("删除单个文档失败", e);
            throw e;
        }
    }
    
    /**
     * 删除多个文档
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @return 删除结果
     */
    public DeleteResult deleteMany(String collectionName, Bson filter) {
        return deleteMany(collectionName, filter, new DeleteOptions());
    }
    
    /**
     * 删除多个文档（带删除选项）
     * @param collectionName 集合名
     * @param filter 过滤条件
     * @param options 删除选项
     * @return 删除结果
     */
    public DeleteResult deleteMany(String collectionName, Bson filter, DeleteOptions options) {
        try {
            return getCollection(collectionName).deleteMany(filter, options);
        } catch (Exception e) {
            logger.error("删除多个文档失败", e);
            throw e;
        }
    }
    
    // -------------------------- 索引操作 --------------------------
    
    /**
     * 创建索引
     * @param collectionName 集合名
     * @param keys 索引键
     * @param options 索引选项
     * @return 索引名称
     */
    public String createIndex(String collectionName, Bson keys, IndexOptions options) {
        try {
            return getCollection(collectionName).createIndex(keys, options);
        } catch (Exception e) {
            logger.error("创建索引失败", e);
            throw e;
        }
    }
    
    /**
     * 获取所有索引
     * @param collectionName 集合名
     * @return 索引文档列表
     */
    public List<Document> getIndexes(String collectionName) {
        try {
            return getCollection(collectionName).listIndexes().into(new java.util.ArrayList<>());
        } catch (Exception e) {
            logger.error("获取索引列表失败", e);
            throw e;
        }
    }
    
    /**
     * 删除索引
     * @param collectionName 集合名
     * @param indexName 索引名称
     */
    public void dropIndex(String collectionName, String indexName) {
        try {
            getCollection(collectionName).dropIndex(indexName);
        } catch (Exception e) {
            logger.error("删除索引失败", e);
            throw e;
        }
    }
    
    /**
     * 关闭连接
     */
    public void close() {
        if (mongoClient != null) {
            try {
                mongoClient.close();
                logger.info("MongoDB连接池已关闭");
            } catch (Exception e) {
                logger.error("关闭MongoDB连接池失败", e);
            }
        }
    }
}
    