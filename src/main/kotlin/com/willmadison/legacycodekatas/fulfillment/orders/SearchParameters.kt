package com.willmadison.legacycodekatas.fulfillment.orders

class SearchParameters(val ids: Set<Int> = emptySet(), val statuses: Set<Order.Status> = emptySet(),
                       val orderTypes: Set<OrderType> = emptySet())