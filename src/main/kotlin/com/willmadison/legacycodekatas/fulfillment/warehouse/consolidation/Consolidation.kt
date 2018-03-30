package com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation

interface Consolidation {
    fun status(orderNumber: Int, transactionId: String): ConsolidatableOrder?
}