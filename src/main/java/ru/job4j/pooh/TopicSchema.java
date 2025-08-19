package ru.job4j.pooh;

import java.util.concurrent.*;

public class TopicSchema implements Schema {
    private final ConcurrentHashMap<String, CopyOnWriteArraySet<Receiver>> subscribers = new ConcurrentHashMap<>();
    private final BlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        subscribers.putIfAbsent(receiver.name(), new CopyOnWriteArraySet<>());
        subscribers.get(receiver.name()).add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        messages.offer(message);
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Message message = messages.poll();
                if (message != null) {
                    var topicSubscribers = subscribers.get(message.name());
                    if (topicSubscribers != null && !topicSubscribers.isEmpty()) {
                        for (Receiver subscriber : topicSubscribers) {
                            subscriber.receive(message.text());
                        }
                    }
                } else {
                    condition.off();
                    condition.await();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}