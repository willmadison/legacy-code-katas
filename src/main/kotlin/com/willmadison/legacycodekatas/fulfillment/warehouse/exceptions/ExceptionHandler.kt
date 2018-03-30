package com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions

import com.fasterxml.jackson.databind.ObjectMapper
import com.willmadison.legacycodekatas.fulfillment.orders.*
import com.willmadison.legacycodekatas.fulfillment.orders.SearchParameters
import com.willmadison.legacycodekatas.fulfillment.warehouse.Message
import com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation.Consolidation
import com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions.configuration.ExceptionConfiguration
import com.willmadison.legacycodekatas.fulfillment.warehouse.management.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.util.CollectionUtils
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors

@Service
class ExceptionHandler(private val orderService: OrderService, private val wms: WarehouseManagement, private val consolidation: Consolidation,
                       private val queue: Queue<Message>, private val configuration: ExceptionConfiguration) {

    private val objectMapper = ObjectMapper()

    companion object {
        const val MAX_BACKGROUND_EXCEPTION_HANDLERS = 8
    }


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

        val wipOrders = orderService.find(searchParameters)

        val singletons = mutableListOf<Order>()
        val multiLineOrders = mutableListOf<Order>()

        if (wipOrders.isNotEmpty()) {
            logger.info("Found {} WIP {} orders! Preparing to handle exceptions...", wipOrders.size, orderType)

            for (order in wipOrders) {
                val reservationId = order.reservationId
                val orderNumber = order.number

                val consolidatedOrder = consolidation.status(orderNumber, order.transactionId)

                val isConsolidatableOrder = (reservationId.isNotEmpty() &&
                        !reservationId.endsWith("-X")) || consolidatedOrder != null

                when {
                    isConsolidatableOrder -> multiLineOrders.add(order)
                    else -> singletons.add(order)
                }
            }

            var completions = 0
            var submissions = 0

            val backgroundExceptionHandlers = Executors.newFixedThreadPool(MAX_BACKGROUND_EXCEPTION_HANDLERS)


            val exceptionHandlingService = ExecutorCompletionService<Boolean>(backgroundExceptionHandlers)

            exceptionHandlingService.submit {
                handleSingleLineItemOrderExceptions(singletons, orderType, configuration)
                true
            }
            ++submissions

            exceptionHandlingService.submit {
                handleConsolidateableOrderExceptions(multiLineOrders, orderType, configuration)
                true
            }
            ++submissions

            while (completions < submissions) {
                try {
                    exceptionHandlingService.take()
                    ++completions
                } catch (e: Exception) {
                    logger.error("Encountered an exception attempting to retrieve our exception handling results....", e)
                }

            }
        }
    }

    private fun handleSingleLineItemOrderExceptions(orders: Collection<Order>, orderType: OrderType,
                                                    configuration: ExceptionConfiguration) {
        logger.info("Handling exceptions for {} non-consolidatable {} orders...", orders.size, orderType)

        var numOrdersProcessed = 0

        val orderVerificationsByOrderNumber = HashMap<Int, OrderVerification>()
        val picksByOrderItemId = HashMap<String, MutableCollection<Pick>>()

        for (order in orders) {
            val orderNumber = order.number

            val searchParameters = com.willmadison.legacycodekatas.fulfillment.warehouse.management.SearchParameters(orderNumber = orderNumber)

            try {
                logger.info("Looking up order verification status for {} Order #{} (primeTid: {})", orderType, orderNumber, order.transactionId)

                val orderVerificationSearch = OrderVerificationSearchRequest(searchParameters, order.transactionId)
                val orderVerificationSearchResponse = wms.search(orderVerificationSearch)

                val orderVerifications = orderVerificationSearchResponse.verifications

                if (orderVerifications.isNotEmpty()) {
                    logger.info("Found {} order verification records for {} Order #{} (primeTid: {})", orderVerifications.size,
                            orderType, orderNumber, order.transactionId)

                    for (verification in orderVerifications) {
                        if (verification.successful) {
                            orderVerificationsByOrderNumber[orderNumber] = verification
                            break
                        }
                    }
                } else {
                    logger.warn("No order verifications for {} Order #{}! (primeTid: {})", orderType, orderNumber, order.transactionId)
                }
            } catch (e: Exception) {
                logger.info("Encountered an exception attempting to retrieve the order verification status for {} Order #{}! (primeTid: {})",
                        orderType, orderNumber, order.transactionId, e)
            }

            try {
                logger.info("Looking up picks for {} Order #{} (primeTid: {})", orderType, orderNumber, order.transactionId)

                val pickSearchRequest = PickSearchRequest(searchParameters, order.transactionId)
                val pickSearchResponse = wms.search(pickSearchRequest)

                val picks = pickSearchResponse.picks

                if (!CollectionUtils.isEmpty(picks)) {
                    logger.info("Found {} picks for {} Order #{} (primeTid: {})", picks.size, orderType, orderNumber, order.transactionId)

                    for (pick in picks) {
                        val orderItemId = pick.orerItemId

                        if (!picksByOrderItemId.containsKey(orderItemId)) {
                            picksByOrderItemId[orderItemId] = ArrayList()
                        }

                        picksByOrderItemId[orderItemId]?.add(pick)
                    }
                } else {
                    logger.warn("No picks found for {} Order #{}! (primeTid: {})", orderType, orderNumber, order.transactionId)
                }
            } catch (e: Exception) {
                logger.info("Encountered an exception attempting to retrieve the picks for {} Order #{}! (primeTid: {})",
                        orderType, orderNumber, order.transactionId, e)
            }

        }

        val repickableStatuses = EnumSet.of<OrderItem.Status>(OrderItem.Status.WIP,
                OrderItem.Status.STRAGGLED, OrderItem.Status.PICKED)

        for (order in orders) {
            val orderNumber = order.number


            var allItemsShipped = true

            for (item in order.items) {
                if (OrderItem.Status.DELETED.equals(item.status)) {
                    continue
                }

                if (!item.shipped) {
                    allItemsShipped = false
                    break
                }
            }

            val verified = orderVerificationsByOrderNumber.containsKey(orderNumber)

            if (verified) {
                if (allItemsShipped) {
                    order.status = Order.Status.COMPLETE
                    order.completionDate = LocalDateTime.now(ZoneId.of("UTC"))
                } else {
                    logger.info("{} Order #{} has been scan verified but not all items have shipped. Leaving in WIP status... " + "(primeTid: {})", orderType, orderNumber, order.transactionId)
                }
            } else {
                logger.info("{} Order #{} has not completed scan verification. Checking for auto-repick candidates... (primeTid: {})", orderType, orderNumber, order.transactionId)

                val maxAutoRepicks = configuration.maxAutoStraggles

                for (item in order.items) {
                    if (!repickableStatuses.contains(item.status)) {
                        logger.info("Order Item {} on {} Order #{} is in {} status which is not a repickable status! Skipping! (primeTid: {})",
                                item.id, order.type, order.number, item.status, order.transactionId)
                        continue
                    }

                    val orderItemId = item.id

                    val picksForItem = picksByOrderItemId[orderItemId]

                    if (picksForItem != null) {
                        var mostRecentPick: Pick? = null

                        for (pick in picksForItem) {
                            if (mostRecentPick == null || pick.updateDate.isAfter(mostRecentPick.updateDate)) {
                                mostRecentPick = pick
                            }
                        }

                        if (mostRecentPick == null) {
                            continue
                        }

                        val lastUpdate = mostRecentPick.updateDate.atZone(ZoneId.of("UTC"))
                        val autoRepickTimeFrame = Duration.ofMinutes(configuration.autoStraggleTimeframeMinutes)
                        var autoRepickTimeThreshold = lastUpdate.plus(autoRepickTimeFrame).toLocalDateTime()

                        val now = LocalDateTime.now(ZoneId.of("UTC"))
                        autoRepickTimeThreshold = autoRepickTimeThreshold.atZone(ZoneId.of("UTC")).toLocalDateTime()

                        //TODO: We need to update this to utilize the release date (when added) if we're currently batch aware...
                        val pickWasWorked = mostRecentPick.status != null || mostRecentPick.wmsUserId != null
                        val repickTimeframePassed = now.isAfter(autoRepickTimeThreshold)

                        val suspended = mostRecentPick.status != null && Pick.Status.SUSPENDED == mostRecentPick.status
                        val pickDeemedOut = mostRecentPick.straggled && mostRecentPick.skill.stragglerSkill == null && suspended

                        val performAutoRepick = pickWasWorked && item.released && repickTimeframePassed && !pickDeemedOut

                        if (performAutoRepick) {
                            logger.warn("Preparing to attempt to auto repick Order Item {} on {} Order #{}! (Last Update {}) (primeTid: {}).", item.id,
                                    orderType, order.number, lastUpdate, order.transactionId)

                            if (configuration.autoStraggleEnabled) {
                                var numRepicks = item.numStraggles

                                if (numRepicks < maxAutoRepicks) {
                                    val pickSkill = mostRecentPick.skill

                                    var repickSkill: Skill?

                                    if (pickSkill.stragglerSkill != null) {
                                        repickSkill = pickSkill.stragglerSkill
                                    } else {
                                        repickSkill = pickSkill
                                    }

                                    mostRecentPick.skill = repickSkill
                                    mostRecentPick.status = null
                                    mostRecentPick.wmsUserId = null
                                    mostRecentPick.straggled = true
                                    mostRecentPick.updateDate = LocalDateTime.now(ZoneId.of("UTC"))
                                    mostRecentPick.quantity = 0.0

                                    try {
                                        val request = PickSaveRequest(mostRecentPick, order.transactionId)
                                        wms.save(request)
                                        item.numStraggles = ++numRepicks
                                    } catch (e: Exception) {
                                        logger.info("Encountered an error attempting to re-pick the most recent pick  for Order Item {} on {} Order #{}! (primeTid: {})", item.id, orderType,
                                                order.number, order.transactionId, e)
                                    }

                                } else {
                                    logger.warn("Unable to auto repick Order Item {} on {} Order #{}! Item has already been repicked" + "{} time(s). (Max # of automatic repicks {}) (primeTid: {})!", item.id,
                                            orderType, order.number, numRepicks, maxAutoRepicks, order.transactionId)
                                }
                            } else {
                                logger.info("Found auto-repick eligible Pick {} for Order Item {} on Order #{}. Auto-repick is disabled. (primeTid: {})",
                                        mostRecentPick.id, item.id, orderType, order.number, order.transactionId)
                            }
                        } else {
                            logger.info("No need to auto-repick Pick {} for Order Item {} on {} Order #{}. " + "(Pick Was Worked? {}, Item Released? {}, Pick Deemed Out By Straggler? {}, Last Update: {}) (primeTid: {})",
                                    mostRecentPick.id, item.id, orderType, order.number,
                                    pickWasWorked, item.released, pickDeemedOut, lastUpdate, order.transactionId)
                        }
                    } else {
                        logger.warn("No picks found for Item {} on {} Order #{}! (primeTid: {})", orderItemId, orderType,
                                orderNumber, order.transactionId)
                    }
                }
            }

            orderService.save(order)
            ++numOrdersProcessed
        }

        logger.info("{} exceptions handled of the {} non-consolidateable {} orders...", numOrdersProcessed, orders.size, orderType)
    }


    private fun handleConsolidateableOrderExceptions(orders: Collection<Order>, orderType: OrderType, configuration: ExceptionConfiguration) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }


}