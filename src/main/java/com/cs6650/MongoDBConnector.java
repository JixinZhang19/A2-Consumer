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
        // MongoClient maintains an internal connection pool that can handle multiple concurrent requests
        // and automatically manages the creation, reuse, and release of connections
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToConnectionPoolSettings(builder ->
                        builder.minSize(100)
                                .maxSize(300)
                                .maxConnectionIdleTime(180000, TimeUnit.MILLISECONDS)
                                .maxConnectionLifeTime(300000, TimeUnit.MILLISECONDS)
                )
                .writeConcern(WriteConcern.UNACKNOWLEDGED)
                .build();
        mongoClient = MongoClients.create(settings);
        database = mongoClient.getDatabase(databaseName);
        // Check-before-create mechanism (atomicity: when inserting concurrently, the first arriving request creates
        // the collection and inserts the data, and other concurrent requests wait for the collection to be created
        // before continuing the insertion operation
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