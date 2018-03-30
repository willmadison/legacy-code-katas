package com.willmadison.legacycodekatas.fulfillment.orders

enum class OrderType(val typeId: Int, val description: String) {
    B2C(1, "Business to Customer"),
    B2B(2, "Business to Business (usually bulk)"),
    LARGE_NON_SORTABLE(3, "Large and/or oddly shaped products")
}