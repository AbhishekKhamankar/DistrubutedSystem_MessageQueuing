package DistrubutedSystem_MessageQueuing;

//implement a distributed system for message queuing,
//Description: create a message queue system where producer send message and multiple consumers can process them.

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


class MessageQueue {
    private final BlockingQueue<String> queue;

    public MessageQueue(int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    public void sendMessage(String message) throws InterruptedException {
        queue.put(message);
        System.out.println("Produced: " + message);
    }

    public String receiveMessage() throws InterruptedException {
        String message = queue.take();
        System.out.println("Consumed: " + message);
        return message;
    }
}

class Producer extends Thread {
    private final MessageQueue messageQueue;
    private final int producerId;

    public Producer(MessageQueue messageQueue, int producerId) {
        this.messageQueue = messageQueue;
        this.producerId = producerId;
    }

    @Override
    public void run() {
        try {
            for (int i = 1; i <= 10; i++) {
                String message = "Message " + i + " from Producer " + producerId;
                messageQueue.sendMessage(message);
                Thread.sleep((int) (Math.random() * 500));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Producer " + producerId + " interrupted");
        }
    }
}

class Consumer extends Thread {
    private final MessageQueue messageQueue;
    private final int consumerId;

    public Consumer(MessageQueue messageQueue, int consumerId) {
        this.messageQueue = messageQueue;
        this.consumerId = consumerId;
    }

    @Override
    public void run() {
        try {
            while (true) {
                String message = messageQueue.receiveMessage();
                System.out.println("Consumer " + consumerId + " processed: " + message);
                Thread.sleep((int) (Math.random() * 600)); // processing delay
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Consumer " + consumerId + " interrupted");
        }
    }
}


public class DistributedMessageQueueSystem {
    public static void main(String[] args) {
        int queueCapacity = 5; // Capacity of the message queue
        MessageQueue messageQueue = new MessageQueue(queueCapacity);

        // Create Producers
        Producer producer1 = new Producer(messageQueue, 1);
        Producer producer2 = new Producer(messageQueue, 2);

        // Create Consumers
        Consumer consumer1 = new Consumer(messageQueue, 1);
        Consumer consumer2 = new Consumer(messageQueue, 2);
        Consumer consumer3 = new Consumer(messageQueue, 3);

        // Start Producers
        producer1.start();
        producer2.start();

        // Start Consumers
        consumer1.start();
        consumer2.start();
        consumer3.start();

        //Producers to finish
        try {
            producer1.join();
            producer2.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Wait for Consumers to process messages
        System.out.println("All producers finished. Waiting for consumers to complete...");
        try {
            Thread.sleep(5000); // finish processing
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("System shutting down.");
        System.exit(0);
    }
}