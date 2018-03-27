package com.willmadison.legacycodekatas.fulfillment.orders

class DefaultOrderService : OrderService {
    override fun find(searchParameters: SearchParameters): Set<Order> = emptySet()
}