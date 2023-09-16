package com.demo.kinesis;

import com.demo.kinesis.model.CartAbandonmentEvent;
import com.demo.kinesis.model.CartItem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@code EventsManager} class is responsible for managing cart abandonment events in an e-commerce system.
 *
 * <p>This class maintains a collection of cart abandonment events and performs operations such as identifying
 * abandoned carts based on a provided abandonment time threshold, generating reminder messages for abandoned
 * carts, and calculating the average potential order size for abandoned carts.
 */
public class EventsManager {
    static final Logger logger = LogManager.getLogger(EventsManager.class);

    private final Set<CartAbandonmentEvent> events;

    public EventsManager() {
        this.events = new HashSet<>();
    }

    /**
     * Adds a cart abandonment event to the manager.
     *
     * @param event The cart abandonment event to add.
     */
    public void addEvent(CartAbandonmentEvent event) {
        events.add(event);
    }

    /**
     * Finds abandoned carts based on the provided abandonment time threshold and sends reminders.
     *
     * @param abandonmentTimeThreshold The threshold time for cart abandonment.
     * @return A set of abandoned cart events.
     */
    private Set<CartAbandonmentEvent> findAbandonedCarts(long abandonmentTimeThreshold) {
        Set<CartAbandonmentEvent> abandonmentEvents = new HashSet<>();
        if (!events.isEmpty()) {
            for (CartAbandonmentEvent event : events) {
                if (event.getTimestamp() < abandonmentTimeThreshold) {
                    abandonmentEvents.add(event);
                    String reminderMessage = generateReminderMessage(event);
                    sendNotification(reminderMessage);
                }
            }
        }
        return abandonmentEvents;
    }

    /**
     * Generates a reminder message for an abandoned cart event.
     *
     * @param event The abandoned cart event.
     * @return The reminder message.
     */
    private String generateReminderMessage(CartAbandonmentEvent event) {
        return String.format("Reminder: User %s abandoned their cart with %d items ",
                event.getCustomerID(),
                event.getCartItems().stream().mapToInt(CartItem::getProductQuantity).sum());
    }

    /**
     * Sends a notification message (e.g., email) to the user.
     *
     * @param message The notification message.
     */
    private void sendNotification(String message) {
        // In a real project, the logic to send a notification (e.g., email) to the user to be implemented.
        logger.info("Notification sent: " + message);
    }

    /**
     * Calculates the average potential order size for abandoned carts.
     *
     * @param currentTimeMillis        The current time in milliseconds.
     * @param abandonmentTimeThreshold The threshold time for cart abandonment.
     * @return The calculated average potential order size.
     */
    public double calculateAvgPotentialOrderSize(long currentTimeMillis, long abandonmentTimeThreshold) {
        logger.info("Current time in milliseconds: " + currentTimeMillis);
        logger.info("Threshold time in milliseconds: " + abandonmentTimeThreshold);

        long thresholdTimeMillis = Instant.ofEpochMilli(currentTimeMillis)
                .minus(Duration.ofMinutes(abandonmentTimeThreshold)).toEpochMilli();

        // Find abandoned cart events and send notifications
        Set<CartAbandonmentEvent> abandonmentEvents = findAbandonedCarts(thresholdTimeMillis);

        double avgPotentialOrderSize = 0.0;
        if (!abandonmentEvents.isEmpty()) {
            long abandonedCarts = abandonmentEvents.size();
            long allCarts = events.size();
            logger.info("All carts: " + allCarts);
            logger.info("Abandoned carts: " + abandonedCarts);
            logger.info(String.format("Percentage of abandoned carts: %.2f", ((double) abandonedCarts / allCarts) * 100));

            // Calculate average potential order size
            double totalPotentialSales = abandonmentEvents.stream()
                    .flatMap(event -> event.getCartItems().stream())
                    .mapToDouble(orderItem -> orderItem.getProductPrice() * orderItem.getProductQuantity())
                    .sum();

            avgPotentialOrderSize = totalPotentialSales / abandonedCarts;
        }
        return avgPotentialOrderSize;
    }
}
