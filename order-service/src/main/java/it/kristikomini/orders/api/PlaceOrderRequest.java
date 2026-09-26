package it.kristikomini.orders.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.math.BigDecimal;

public record PlaceOrderRequest(
        @NotBlank String sku,
        @Positive int quantity,
        @NotNull @PositiveOrZero BigDecimal unitPrice) {
}
