package com.willmadison.legacycodekatas.fulfillment.warehouse.management

import java.time.LocalDateTime

class Pick(val id: Int,
           val orerItemId: String,
           var updateDate: LocalDateTime,
           var status: Status?,
           var wmsUserId: String?,
           var straggled: Boolean,
           var skill: Skill
) {
    var quantity: Double = 0.0

    enum class Status {
        SUSPENDED
    }
}
