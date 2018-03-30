package com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation

import java.time.LocalDateTime

class ConsolidatableOrder(val items: Collection<ConsolidatableOrderItem>)

class ConsolidatableOrderItem(val id: String, var lastUpdate: LocalDateTime, val placed: Boolean) {
}