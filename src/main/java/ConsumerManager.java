import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * @author Rebecca Zhang
 * Created on 2024-06-27
 */
public class ConsumerManager {

    // todo: tuning QUEUE_COUNT and CONSUMER_PER_QUEUE
    private static final int QUEUE_COUNT = 500; // 与 server 一致
    private static final int CONSUMER_PER_QUEUE = 1;
    private static final int CONSUMER_COUNT = QUEUE_COUNT * CONSUMER_PER_QUEUE;
    private static final String HOST = "localhost";

    private final Connection connection;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<Integer, LifeRide> hashMap;

    public ConsumerManager() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);

        this.connection = factory.newConnection();
        this.executorService = Executors.newFixedThreadPool(CONSUMER_COUNT);
        this.hashMap = new ConcurrentHashMap<>();
    }

    public void startConsumers() {
        for (int i = 0; i < QUEUE_COUNT; i++) {
            String queueName = "queue_" + i;
            for (int j = 0; j < CONSUMER_PER_QUEUE; j++) {
                MessageConsumer consumer = new MessageConsumer(connection, queueName, hashMap);
                executorService.submit(consumer);
            }
        }
        System.out.println("All message consumers are now running.");
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    System.err.println("Error: executorService could not terminate!");
                }
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        try {
            // 所有与之相关的 channel 会自动关闭
            connection.close();
        } catch (Exception e) {
            System.err.println("Error: failed to close RabbitMQ connection!");
        }
        System.out.println("All message consumers have been shutdown.");
    }

}