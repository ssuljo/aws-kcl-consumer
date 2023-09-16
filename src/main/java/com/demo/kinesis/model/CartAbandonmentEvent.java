package com.demo.kinesis.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@code CartAbandonmentEvent} class represents an event that captures information
 * about a cart abandonment incident in an e-commerce system. It provides details
 * about the items in the cart, the customer who abandoned the cart, the seller's ID,
 * and the timestamp of the abandonment event.
 * <p>
 * Instances of this class are used to record and analyze cart abandonment behavior
 * within the e-commerce platform, enabling the identification of trends and the
 * development of strategies to recover abandoned carts.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CartAbandonmentEvent {

    @JsonProperty("cart_items")
    private List<CartItem> cartItems = new ArrayList<>();

    @JsonProperty("customer_id")
    private String customerID;

    @JsonProperty("seller_id")
    private String sellerID;

    @JsonProperty("event_time")
    private long timestamp;
}
