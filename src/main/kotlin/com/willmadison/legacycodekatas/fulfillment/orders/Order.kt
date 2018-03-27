package com.willmadison.legacycodekatas.fulfillment.orders

import java.util.HashMap

class Order {
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