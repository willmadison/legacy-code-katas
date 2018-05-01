package com.willmadison.legacycodekatas.examples.sprouting;

import com.willmadison.legacycodekatas.fulfillment.orders.Order;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;


public class PostSproutMethodTest {

    private PostSproutMethod postSproutMethod;

    @Before
    public void setUp() throws Exception {
        postSproutMethod = new PostSproutMethod();
    }

    @Test
    public void staleOrders() {
        Order o1 = new Order();
        o1.lastUpdate = LocalDateTime.now();

        Order o2 = new Order();
        o2.lastUpdate = LocalDateTime.now().minusHours(5);

        Collection<Order> orders = Arrays.asList(o1, o2);

        Collection<Order> stales = postSproutMethod.staleOrders(orders);

        assertThat(stales.size()).isEqualTo(1);
        assertThat(stales.iterator().next()).isEqualTo(o2);
    }
}