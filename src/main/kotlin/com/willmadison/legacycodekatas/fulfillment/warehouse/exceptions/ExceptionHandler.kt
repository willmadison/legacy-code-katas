package com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions

import com.fasterxml.jackson.databind.ObjectMapper
import com.willmadison.legacycodekatas.fulfillment.orders.Order
import com.willmadison.legacycodekatas.fulfillment.orders.OrderService
import com.willmadison.legacycodekatas.fulfillment.orders.OrderType
import com.willmadison.legacycodekatas.fulfillment.orders.SearchParameters
import com.willmadison.legacycodekatas.fulfillment.warehouse.Consolidation
import com.willmadison.legacycodekatas.fulfillment.warehouse.Message
import com.willmadison.legacycodekatas.fulfillment.warehouse.WarehouseManagement
import com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions.configuration.ExceptionConfiguration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors

@Service
class ExceptionHandler(private val orders: OrderService, private val wms: WarehouseManagement, private val consolidation: Consolidation,
                       private val queue: Queue<Message>, private val configuration: ExceptionConfiguration) {

    private val objectMapper = ObjectMapper()

    private val logger: Logger = LoggerFactory.getLogger(ExceptionHandler::class.java)

    @Scheduled(cron = "0 0/1 * * * *")  // Every minute...
    fun handleExceptions() {
        logger.info("Handling exception scenarios...")

        if (configuration.enabled) {
            if (configuration.warehouseOperational) {
                val orderTypes = EnumSet.noneOf(OrderType::class.java)

                for (orderType in configuration.supportedOrderTypes) {
                    orderTypes.add(orderType)
                }

                val backgroundOrderExceptionHandlers = Executors.newFixedThreadPool(OrderType.values().size)

                val exceptionHandlingCompletionService = ExecutorCompletionService<Boolean>(backgroundOrderExceptionHandlers)

                var completions = 0
                var submissions = 0

                for (orderType in orderTypes) {
                    exceptionHandlingCompletionService.submit {
                        handleExceptionsFor(orderType, configuration)
                        true
                    }
                    ++submissions
                }

                logger.info("Awaiting exception handling completion...", orderTypes)

                while (completions < submissions) {
                    try {
                        exceptionHandlingCompletionService.take()
                        ++completions
                    } catch (e: Exception) {
                        logger.error("Encountered an exception attempting to retrieve our exception handling results....", e)
                    }

                }

                logger.info("Exceptions completely handled for the following order types: {}...", orderTypes)
            } else {
                logger.info("No need to handle exceptions at this moment. Warehouse is not operational!")
            }
        } else {
            logger.info("Exception handling is disabled. Unable to handle exception scenarios!")
        }
    }

    private fun handleExceptionsFor(orderType: OrderType, configuration: ExceptionConfiguration) {
        logger.info("Handling {} order exceptions....", orderType)

        logger.info("Looking up WIP {} orders with one or more lines in WIP status....", orderType)

        val searchParameters = SearchParameters(orderTypes = setOf(orderType), statuses = setOf(Order.Status.WIP))

        val wipOrders = orders.find(searchParameters)
    }


}