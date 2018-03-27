package com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions.configuration

import com.willmadison.legacycodekatas.fulfillment.orders.OrderType

open class ExceptionConfiguration(var enabled: Boolean = true,
                                  var warehouseOperational: Boolean = true,
                                  var supportedOrderTypes: Set<OrderType> = OrderType.values().toSet())