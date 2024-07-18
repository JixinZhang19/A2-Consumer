package com.cs6650;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.concurrent.TimeUnit;

/**
 * @author Rebecca Zhang
 * Created on 2024-07-15
 */
public class MongoDBConnector implements AutoCloseable {

    private final MongoClient mongoClient;
    private final MongoDatabase database;
    private final MongoCollection<Document> collection;

    public MongoDBConnector(String connectionString, String databaseName, String collectionName) throws MongoException {
        /**
         * MongoClient 内部维护了一个连接池，可以处理多个并发请求，会自动管理连接的创建、复用和释放
         * 内置连接池配置
         * minSIze: 保持足够的最小连接数以应对突发流量
         * maxSize: 设置为线程数，确保每个线程都能获得连接
         * maxConnectionIdleTime: 回收空闲线程
         * maxConnectionLifeTime: 限制连接的最大生命周期，有助于防止连接过期
         */
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToConnectionPoolSettings(builder ->
                        builder.minSize(100)
                                .maxSize(300)
                                .maxConnectionIdleTime(180000, TimeUnit.MILLISECONDS)
                                .maxConnectionLifeTime(300000, TimeUnit.MILLISECONDS)
                )
                .writeConcern(WriteConcern.UNACKNOWLEDGED) // W0 不等待写入确认，风险较高；W1 等待主节点确认写入操作完成（默认写关注级别）
                .build();
        mongoClient = MongoClients.create(settings);
        database = mongoClient.getDatabase(databaseName);
        // 先检查后创建机制，原子性：并发插入时，第一个到达的请求会创建集合并插入数据，其他并发请求等待集合创建完成，再继续插入操作
        collection = database.getCollection(collectionName);
    }

    public void insertLifeRide(LifeRide lifeRide) {
        Document document = new Document("resortID", lifeRide.getResortID())
                .append("seasonID", lifeRide.getSeasonID())
                .append("dayID", lifeRide.getDayID())
                .append("skierID", lifeRide.getSkierID())
                .append("time", lifeRide.getTime())
                .append("liftID", lifeRide.getLiftID());

        collection.insertOne(document);
    }

    @Override
    public void close() {
        mongoClient.close();
    }

}