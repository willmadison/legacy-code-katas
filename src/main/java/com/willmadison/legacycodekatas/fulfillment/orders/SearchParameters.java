package com.willmadison.legacycodekatas.fulfillment.orders;

import java.util.Collection;
import java.util.Set;

public class SearchParameters {

    public Set<Integer> ids;

    public Set<Order.Status> orderStatuses;

    public Set<Order.Type> orderTypes;

    public Collection<Integer> orderNumbers;
}
