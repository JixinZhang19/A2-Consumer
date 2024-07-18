package com.cs6650;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author Rebecca Zhang
 * Created on 2024-06-26
 */
// todo: queue 大量堆积 ->
//  client 端拦截器
//  增加 consumer 数量和 prefetch 数
//  提高 consumer instance 的性能
//  提高 DB instance 的性能
//  写入确认级别：WriteConcern.UNACKNOWLEDGED
//  消息消费确认：auto ack
public class MessageConsumer implements Runnable {

    private static final Gson gson = new Gson();
    private final Connection connection;
    private final String queueName;
    private final MongoDBConnector mongoDBConnector;

    public MessageConsumer(Connection connection, String queueName, MongoDBConnector mongoDBConnector) {
        this.connection = connection;
        this.queueName = queueName;
        this.mongoDBConnector = mongoDBConnector;
    }

    @Override
    public void run() {
        try {
            Channel channel = connection.createChannel();
            // Fairness guarantee for multiple consumers
            channel.basicQos(20);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                try {
                    processMessage(message);
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (Exception e) {
                    System.err.println("Error: re-put message to RabbitMQ!");
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                }
            };

            channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {});
        } catch (IOException e) {
            System.err.println("Error: failed to consume message!");
        }
    }

    private void processMessage(String message) {
        // Implement business logic
        LifeRide lifeRide = gson.fromJson(message, LifeRide.class);
        mongoDBConnector.insertLifeRide(lifeRide);
    }

}
