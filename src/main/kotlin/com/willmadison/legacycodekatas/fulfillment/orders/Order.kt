package com.willmadison.legacycodekatas.fulfillment.orders

import java.time.LocalDateTime
import java.util.*

class Order(val id: String, val number: Int, val type: OrderType,
            val reservationId: String, val items: Collection<OrderItem>,
            var status: Status, val transactionId: String, var completionDate: LocalDateTime) {

    enum class Status(val statusId: Int) {
        NEW(1),
        REPLENISHING(2),
        READY(3),
        RESERVED(4),
        WIP(5),
        SPLIT(6),
        COMPLETE(7),
        CANCELLED(8);

        private val statusesById = HashMap<Int, Status>()

        init {
            populateStatusByIdLookup();
        }

        private fun populateStatusByIdLookup() {
            for (status in values()) {
                statusesById[status.statusId] = status
            }
        }

        fun byId(statusId: Int): Status {
            return statusesById.get(statusId) ?: NEW
        }
    }
}