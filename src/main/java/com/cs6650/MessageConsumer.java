package com.cs6650;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Rebecca Zhang
 * Created on 2024-06-26
 */
public class MessageConsumer implements Runnable {

    private static final Gson gson = new Gson();
    private final Connection connection;
    private final String queueName;
    private final ConcurrentHashMap<Integer, LifeRide> hashMap;

    public MessageConsumer(Connection connection, String queueName, ConcurrentHashMap<Integer, LifeRide> hashMap) {
        this.connection = connection;
        this.queueName = queueName;
        this.hashMap = hashMap;
    }

    @Override
    public void run() {
        try {
            Channel channel = connection.createChannel();
            // 多消费者时公平性保证，增加整体的 deliver rate
            // todo: tuning prefetchSize
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

            // String basicConsume(String queueName, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException;
            channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {});
        } catch (IOException e) {
            System.err.println("Error: failed to consume message!");
        }
    }

    private void processMessage(String message) {
        // 实现消息处理逻辑
        LifeRide lifeRide = gson.fromJson(message, LifeRide.class);
        hashMap.put(lifeRide.skierID, lifeRide);
    }

}
