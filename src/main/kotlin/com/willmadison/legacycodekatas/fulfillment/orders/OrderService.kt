package com.willmadison.legacycodekatas.fulfillment.orders

interface OrderService {
    fun find(searchParameters: SearchParameters): Set<Order>
    fun save(order: Order)
}