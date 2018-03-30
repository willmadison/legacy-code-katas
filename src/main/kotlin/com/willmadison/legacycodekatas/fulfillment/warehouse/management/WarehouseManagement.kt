package com.willmadison.legacycodekatas.fulfillment.warehouse.management

interface WarehouseManagement {
    fun search(searchRequest: OrderVerificationSearchRequest): OrderVerificationSearchResponse
    fun search(searchRequest: PickSearchRequest): PickSearchResponse
    fun save(request: PickSaveRequest): PickSaveResponse
}