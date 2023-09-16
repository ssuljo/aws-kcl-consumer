package com.demo.kinesis.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The {@code CartItem} class represents an individual item within a shopping cart in an e-commerce system.
 * It includes details such as the product name, product code, quantity, and price of the item.
 * <p>
 * Instances of this class are used to model and manage the contents of a user's shopping cart,
 * facilitating the calculation of cart totals and order processing.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CartItem {
    @JsonProperty("product_name")
    private String productName;

    @JsonProperty("product_code")
    private String productCode;

    @JsonProperty("product_quantity")
    private int productQuantity;

    @JsonProperty("product_price")
    private double productPrice;
}
