package com.willmadison.legacycodekatas.fulfillment.orders

class DefaultOrderService : OrderService {

    override fun find(searchParameters: SearchParameters): Set<Order> = emptySet()

    override fun save(order: Order) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}