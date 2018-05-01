package com.willmadison.legacycodekatas.fulfillment.orders;

import java.util.HashSet;
import java.util.Set;

public class DefaultOrderService implements OrderService {

    @Override
    public Set<Order> find(SearchParameters searchParameters) {
        return new HashSet<>();
    }

    @Override
    public void save(Order order) {

    }

    @Override
    public void save(OrderItem orderItem) {

    }
}
