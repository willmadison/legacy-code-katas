package com.willmadison.legacycodekatas.fulfillment.orders;

import java.util.Set;

public interface OrderService {
    Set<Order> find(SearchParameters searchParameters);
    void save(Order order);
    void save(OrderItem orderItem);
}
