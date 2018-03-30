package com.willmadison.legacycodekatas.fulfillment.orders

class OrderItem(val id: String, val status: Status, val shipped: Boolean, val released: Boolean) {
    var numStraggles: Int = 0

    enum class Status {
        WIP,
        STRAGGLED,
        PICKED,
        DELETED

    }

}