package com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.willmadison.legacycodekatas.fulfillment.orders.Order;
import com.willmadison.legacycodekatas.fulfillment.orders.Order.Type;
import com.willmadison.legacycodekatas.fulfillment.orders.OrderItem;
import com.willmadison.legacycodekatas.fulfillment.orders.OrderService;
import com.willmadison.legacycodekatas.fulfillment.orders.SearchParameters;
import com.willmadison.legacycodekatas.fulfillment.warehouse.Message;
import com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation.ConsolidatableOrder;
import com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation.Consolidation;
import com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation.Label;
import com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions.configuration.ExceptionConfiguration;
import com.willmadison.legacycodekatas.fulfillment.warehouse.management.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class ExceptionHandler {

    private static final int MAX_BACKGROUND_EXCEPTION_HANDLERS = 8;

    private final OrderService orderService;

    private final WarehouseManagement wms;

    private final Consolidation consolidation;

    private final Queue<Message> queue;

    private final ExceptionConfiguration configuration;

    private ObjectMapper objectMapper = new ObjectMapper();

    private ExecutorService backgroundExceptionHandlers = Executors.newFixedThreadPool(MAX_BACKGROUND_EXCEPTION_HANDLERS);

    private ExecutorService backgroundOrderExceptionHandlers = Executors.newFixedThreadPool(Type.values().length);

    private Logger logger = LoggerFactory.getLogger(ExceptionHandler.class);

    ExceptionHandler(OrderService orderService, WarehouseManagement wms, Consolidation consolidation, Queue<Message> queue, ExceptionConfiguration configuration) {
        this.orderService = orderService;
        this.wms = wms;
        this.consolidation = consolidation;
        this.queue = queue;
        this.configuration = configuration;
    }

    @Scheduled(cron = "0 0/1 * * * *")  // Every minute...
    private void handleExceptions() {
        logger.info("Handling exception scenarios...");

        if (configuration.enabled) {
            if (configuration.warehouseOperational) {
                Set<Type> orderTypes = EnumSet.noneOf(Type.class);
                orderTypes.addAll(configuration.supportedOrderTypes);

                ExecutorCompletionService<Boolean> exceptionHandlingCompletionService = new ExecutorCompletionService<>(backgroundOrderExceptionHandlers);

                int completions = 0;
                int submissions = 0;

                for (final Type orderType : orderTypes) {
                    exceptionHandlingCompletionService.submit(() -> {
                        handleExceptionsFor(orderType, configuration);
                        return true;
                    });
                    ++submissions;
                }

                logger.info("Awaiting exception handling completion...", orderTypes);

                //noinspection Duplicates
                while (completions < submissions) {
                    try {
                        exceptionHandlingCompletionService.take();
                        ++completions;
                    } catch (Exception e) {
                        logger.error("Encountered an exception attempting to retrieve our exception handling results....", e);
                    }

                }

                logger.info("Exceptions completely handled for the following order types: {}...", orderTypes);
            } else {
                logger.info("No need to handle exceptions at this moment. Warehouse is not operational!");
            }
        } else {
            logger.info("Exception handling is disabled. Unable to handle exception scenarios!");
        }
    }

    private void handleExceptionsFor(Type orderType, ExceptionConfiguration configuration) {
        logger.info("Handling {} order exceptions....", orderType);

        logger.info("Looking up WIP {} orders with one or more lines in WIP status....", orderType);

        SearchParameters searchParameters = new SearchParameters();
        searchParameters.orderTypes = Collections.singleton(orderType);
        searchParameters.orderStatuses = Collections.singleton(Order.Status.WIP);

        Collection<Order> wipOrders = orderService.find(searchParameters);

        Collection<Order> singletons = new ArrayList<>();
        Collection<Order> multiLineOrders = new ArrayList<>();

        if (!wipOrders.isEmpty()) {
            logger.info("Found {} WIP {} orders! Preparing to handle exceptions...", wipOrders.size(), orderType);

            for (Order order : wipOrders) {
                String reservationId = order.reservationId;
                int orderNumber = order.number;

                ConsolidatableOrder consolidatedOrder = consolidation.status(orderNumber, order.transactionId);

                boolean isConsolidatableOrder = (null != reservationId && !"".equals(reservationId) &&
                        !reservationId.endsWith("-X")) || consolidatedOrder != null;

                if (isConsolidatableOrder) {
                    multiLineOrders.add(order);
                } else {
                    singletons.add(order);
                }
            }

            int completions = 0;
            int submissions = 0;

            ExecutorCompletionService<Boolean> exceptionHandlingService = new ExecutorCompletionService<>(backgroundExceptionHandlers);

            exceptionHandlingService.submit(() -> {
                handleSingleLineItemOrderExceptions(singletons, orderType, configuration);
                return true;
            });
            ++submissions;

            exceptionHandlingService.submit(() -> {
                handleConsolidatableOrderExceptions(multiLineOrders, orderType, configuration);
                return true;
            });

            ++submissions;

            //noinspection Duplicates
            while (completions < submissions) {
                try {
                    exceptionHandlingService.take();
                    ++completions;
                } catch (Exception e) {
                    logger.error("Encountered an exception attempting to retrieve our exception handling results....", e);
                }

            }
        }
    }

    private void handleSingleLineItemOrderExceptions(Collection<Order> orders, Type orderType, ExceptionConfiguration configuration) {
        logger.info("Handling exceptions for {} non-consolidatable {} orders...", orders.size(), orderType);

        int numOrdersProcessed = 0;

        Map<Integer, OrderVerification> orderVerificationsByOrderNumber = new HashMap<>();
        Map<String, Collection<Pick>> picksByOrderItemId = new HashMap<>();

        for (Order order : orders) {
            int orderNumber = order.number;

            com.willmadison.legacycodekatas.fulfillment.warehouse.management.SearchParameters searchParameters =
                    new com.willmadison.legacycodekatas.fulfillment.warehouse.management.SearchParameters();
            searchParameters.orderNumber = orderNumber;

            try {
                logger.info("Looking up order verification status for {} Order #{} (primeTid: {})", orderType, orderNumber, order.transactionId);

                OrderVerificationSearchRequest orderVerificationSearch = new OrderVerificationSearchRequest(searchParameters, order.transactionId);
                OrderVerificationSearchResponse orderVerificationSearchResponse = wms.search(orderVerificationSearch);

                Collection<OrderVerification> orderVerifications = orderVerificationSearchResponse.verifications;

                if (!orderVerifications.isEmpty()) {
                    logger.info("Found {} order verification records for {} Order #{} (primeTid: {})", orderVerifications.size(),
                            orderType, orderNumber, order.transactionId);

                    for (OrderVerification verification : orderVerifications) {
                        if (verification.successful) {
                            orderVerificationsByOrderNumber.put(orderNumber, verification);
                            break;
                        }
                    }
                } else {
                    logger.warn("No order verifications for {} Order #{}! (primeTid: {})", orderType, orderNumber, order.transactionId);
                }
            } catch (Exception e) {
                logger.info("Encountered an exception attempting to retrieve the order verification status for {} Order #{}! (primeTid: {})",
                        orderType, orderNumber, order.transactionId, e);
            }

            try {
                logger.info("Looking up picks for {} Order #{} (primeTid: {})", orderType, orderNumber, order.transactionId);

                PickSearchRequest pickSearchRequest = new PickSearchRequest(searchParameters, order.transactionId);
                PickSearchResponse pickSearchResponse = wms.search(pickSearchRequest);

                Collection<Pick> picks = pickSearchResponse.picks;

                if (!CollectionUtils.isEmpty(picks)) {
                    logger.info("Found {} picks for {} Order #{} (primeTid: {})", picks.size(), orderType, orderNumber, order.transactionId);

                    for (Pick pick : picks) {
                        String orderItemId = pick.orderItemId;

                        if (!picksByOrderItemId.containsKey(orderItemId)) {
                            picksByOrderItemId.put(orderItemId, new ArrayList<>());
                        }

                        picksByOrderItemId.get(orderItemId).add(pick);
                    }
                } else {
                    logger.warn("No picks found for {} Order #{}! (primeTid: {})", orderType, orderNumber, order.transactionId);
                }
            } catch (Exception e) {
                logger.info("Encountered an exception attempting to retrieve the picks for {} Order #{}! (primeTid: {})",
                        orderType, orderNumber, order.transactionId, e);
            }

        }

        Set<OrderItem.Status> repickableStatuses = EnumSet.of(OrderItem.Status.WIP,
                OrderItem.Status.STRAGGLED, OrderItem.Status.PICKED);

        for (Order order : orders) {
            int orderNumber = order.number;

            boolean allItemsShipped = true;

            for (OrderItem item : order.items) {
                if (OrderItem.Status.DELETED.equals(item.status)) {
                    continue;
                }

                if (!item.shipped) {
                    allItemsShipped = false;
                    break;
                }
            }

            boolean verified = orderVerificationsByOrderNumber.containsKey(orderNumber);

            if (verified) {
                if (allItemsShipped) {
                    order.status = Order.Status.COMPLETE;
                    order.completedOn = LocalDateTime.now(ZoneId.of("UTC"));
                } else {
                    logger.info("{} Order #{} has been scan verified but not all items have shipped. Leaving in WIP status... " + "(primeTid: {})", orderType, orderNumber, order.transactionId);
                }
            } else {
                logger.info("{} Order #{} has not completed scan verification. Checking for auto-repick candidates... (primeTid: {})", orderType, orderNumber, order.transactionId);

                int maxAutoRepicks = configuration.maxAutoStraggles;

                for (OrderItem item : order.items) {
                    if (!repickableStatuses.contains(item.status)) {
                        logger.info("Order Item {} on {} Order #{} is in {} status which is not a repickable status! Skipping! (primeTid: {})",
                                item.id, order.type, order.number, item.status, order.transactionId);
                        continue;
                    }

                    String orderItemId = item.id;

                    Collection<Pick> picksForItem = picksByOrderItemId.get(orderItemId);

                    if (picksForItem != null) {
                        Pick mostRecentPick = null;

                        for (Pick pick : picksForItem) {
                            if (mostRecentPick == null || pick.lastUpdate.isAfter(mostRecentPick.lastUpdate)) {
                                mostRecentPick = pick;
                            }
                        }

                        if (mostRecentPick == null) {
                            continue;
                        }

                        ZonedDateTime lastUpdate = mostRecentPick.lastUpdate.atZone(ZoneId.of("UTC"));
                        Duration autoRepickTimeFrame = Duration.ofMinutes(configuration.autoStraggleTimeframeMinutes);
                        LocalDateTime autoRepickTimeThreshold = lastUpdate.plus(autoRepickTimeFrame).toLocalDateTime();

                        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
                        autoRepickTimeThreshold = autoRepickTimeThreshold.atZone(ZoneId.of("UTC")).toLocalDateTime();

                        boolean pickWasWorked = mostRecentPick.status != null || mostRecentPick.wmsUserId != null;
                        boolean repickTimeframePassed = now.isAfter(autoRepickTimeThreshold);

                        boolean suspended = mostRecentPick.status != null && Pick.Status.SUSPENDED == mostRecentPick.status;
                        boolean pickDeemedOut = mostRecentPick.straggled && mostRecentPick.skill.stragglerSkill == null && suspended;

                        boolean performAutoRepick = pickWasWorked && item.released && repickTimeframePassed && !pickDeemedOut;

                        if (performAutoRepick) {
                            logger.warn("Preparing to attempt to auto repick Order Item {} on {} Order #{}! (Last Update {}) (primeTid: {}).", item.id,
                                    orderType, order.number, lastUpdate, order.transactionId);

                            if (configuration.autoStraggleEnabled) {
                                int numRepicks = item.numStraggles;

                                if (numRepicks < maxAutoRepicks) {
                                    Skill pickSkill = mostRecentPick.skill;

                                    Skill repickSkill;

                                    if (pickSkill.stragglerSkill != null) {
                                        repickSkill = pickSkill.stragglerSkill;
                                    } else {
                                        repickSkill = pickSkill;
                                    }

                                    mostRecentPick.skill = repickSkill;
                                    mostRecentPick.status = null;
                                    mostRecentPick.wmsUserId = null;
                                    mostRecentPick.straggled = true;
                                    mostRecentPick.lastUpdate = LocalDateTime.now(ZoneId.of("UTC"));
                                    mostRecentPick.quantity = 0.0;

                                    try {
                                        PickSaveRequest request = new PickSaveRequest(mostRecentPick, order.transactionId);
                                        wms.save(request);
                                        item.numStraggles = ++numRepicks;
                                    } catch (Exception e) {
                                        logger.info("Encountered an error attempting to re-pick the most recent pick  for Order Item {} on {} Order #{}! (primeTid: {})", item.id, orderType,
                                                order.number, order.transactionId, e);
                                    }

                                } else {
                                    logger.warn("Unable to auto repick Order Item {} on {} Order #{}! Item has already been repicked" + "{} time(s). (Max # of automatic repicks {}) (primeTid: {})!", item.id,
                                            orderType, order.number, numRepicks, maxAutoRepicks, order.transactionId);
                                }
                            } else {
                                logger.info("Found auto-repick eligible Pick {} for Order Item {} on Order #{}. Auto-repick is disabled. (primeTid: {})",
                                        mostRecentPick.id, item.id, orderType, order.number, order.transactionId);
                            }
                        } else {
                            logger.info("No need to auto-repick Pick {} for Order Item {} on {} Order #{}. " + "(Pick Was Worked? {}, Item Released? {}, Pick Deemed Out By Straggler? {}, Last Update: {}) (primeTid: {})",
                                    mostRecentPick.id, item.id, orderType, order.number,
                                    pickWasWorked, item.released, pickDeemedOut, lastUpdate, order.transactionId);
                        }
                    } else {
                        logger.warn("No picks found for Item {} on {} Order #{}! (primeTid: {})", orderItemId, orderType,
                                orderNumber, order.transactionId);
                    }
                }
            }

            orderService.save(order);
            ++numOrdersProcessed;
        }

        logger.info("{} exceptions handled of the {} non-consolidateable {} orders...", numOrdersProcessed, orders.size(), orderType);

    }

    private void handleConsolidatableOrderExceptions(Collection<Order> orders, Type orderType, ExceptionConfiguration configuration) {
        logger.info("Handling exceptions for {} consolidateable {} orders...", orders.size(), orderType);

        Set<OrderItem.Status> repickableStatuses = EnumSet.of(OrderItem.Status.WIP,
                OrderItem.Status.STRAGGLED, OrderItem.Status.PICKED);

        int numOrdersProcessed = 0;

        for (Order order : orders) {
            int orderNumber = order.number;

            ConsolidatableOrder consolidatableOrder = null;

            logger.info("Looking up consolidation status for {} Order #{}...(primeTid: {})", orderType, orderNumber, order.transactionId);
            consolidatableOrder = consolidation.status(orderNumber, order.transactionId);

            com.willmadison.legacycodekatas.fulfillment.warehouse.management.SearchParameters searchParameters = new com.willmadison.legacycodekatas.fulfillment.warehouse.management.SearchParameters();
            searchParameters.orderNumber = orderNumber;

            Map<String, Collection<Pick>> picksByOrderItemId = new HashMap<>();

            //noinspection Duplicates
            try {
                logger.info("Looking up picks for {} Order #{} (primeTid: {})", orderType, orderNumber, order.transactionId);

                PickSearchRequest pickSearchRequest = new PickSearchRequest(searchParameters, order.transactionId);
                PickSearchResponse pickSearchResponse = wms.search(pickSearchRequest);

                Collection<Pick> picks = pickSearchResponse.picks;

                if (!CollectionUtils.isEmpty(picks)) {
                    logger.info("Found {} picks for {} Order #{} (primeTid: {})", picks.size(), orderType, orderNumber, order.transactionId);

                    for (Pick pick : picks) {
                        String orderItemId = pick.orderItemId;

                        if (!picksByOrderItemId.containsKey(orderItemId)) {
                            picksByOrderItemId.put(orderItemId, new ArrayList<>());
                        }

                        picksByOrderItemId.get(orderItemId).add(pick);
                    }
                } else {
                    logger.warn("No picks found for {} Order #{}! (primeTid: {})", orderType, orderNumber, order.transactionId);
                }
            } catch (Exception e) {
                logger.info("Encountered an exception attempting to retrieve the picks for {} Order #{}! (primeTid: {})",
                        orderType, orderNumber, order.transactionId, e);
            }

            Map<Integer, ConsolidatableOrder.ConsolidatableOrderItem> consolidatedItemsByPickId = new HashMap<>();

            if (consolidatableOrder != null) {
                for (ConsolidatableOrder.ConsolidatableOrderItem item : consolidatableOrder.items) {
                    try {
                        consolidatedItemsByPickId.put(Integer.parseInt(item.id), item);
                    } catch (NumberFormatException nfe) {
                        logger.warn("Encountered NumberFormatException attempting to parse the identifier for the following consolidateable item: {}", item);
                    }

                }

                logger.info("Retrieved consolidateable order with {} items for {} Order #{}. Checking for auto-straggle" + " candidates... (primeTid: {})", consolidatableOrder.items.size(), order.type, orderNumber, order.transactionId);

                int maxAutoRepicks = configuration.maxAutoStraggles;

                for (OrderItem item : order.items) {
                    if (!repickableStatuses.contains(item.status)) {
                        logger.info("Order Item {} on {} Order #{} is in {} status which is not a repickable status! Skipping! (primeTid: {})",
                                item.id, order.type, order.number, item.status, order.transactionId);
                        continue;
                    }

                    String orderItemId = item.id;

                    Collection<Pick> picksForItem = picksByOrderItemId.get(orderItemId);

                    if (picksForItem != null) {
                        ConsolidatableOrder.ConsolidatableOrderItem consolidatedItem = null;
                        Pick repickCandidate = null;

                        for (Pick pick : picksForItem) {
                            consolidatedItem = consolidatedItemsByPickId.get(pick.id);

                            if (consolidatedItem != null) {
                                repickCandidate = pick;
                                break;
                            }
                        }

                        if (consolidatedItem != null && repickCandidate != null) {
                            ZonedDateTime lastUpdate = consolidatedItem.lastUpdate.atZone(ZoneId.of("UTC"));
                            Duration repickTimeframe = Duration.ofMinutes(configuration.autoStraggleTimeframeMinutes);
                            LocalDateTime repickTimeThreshold = lastUpdate.plus(repickTimeframe).toLocalDateTime();
                            repickTimeThreshold = repickTimeThreshold.atZone(ZoneId.of("UTC")).toLocalDateTime();

                            LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));

                            boolean pickWasWorked = repickCandidate.status != null || repickCandidate.wmsUserId != null;
                            boolean repickTimeframePassed = now.isAfter(repickTimeThreshold);

                            boolean suspended = repickCandidate.status != null && Pick.Status.SUSPENDED == repickCandidate.status;
                            boolean pickDeemedOut = repickCandidate.straggled && repickCandidate.skill.stragglerSkill == null && suspended;

                            boolean performAutoRepick = pickWasWorked && item.released && !consolidatedItem.placed && repickTimeframePassed && !pickDeemedOut;

                            if (consolidatedItem.placed) {
                                item.status = OrderItem.Status.PLACED;
                            }

                            if (performAutoRepick) {
                                logger.warn("Preparing to attempt to auto repick Order Item {} on {} Order #{}! (Last Update {}) (primeTid: {}).", item.id,
                                        orderType, order.number, lastUpdate, order.transactionId);

                                if (configuration.autoStraggleEnabled) {
                                    int numRepicks = item.numStraggles;

                                    if (numRepicks < maxAutoRepicks) {
                                        Skill pickSkill = repickCandidate.skill;

                                        Skill repickSkill = pickSkill.stragglerSkill != null ? pickSkill.stragglerSkill : pickSkill;

                                        repickCandidate.skill = repickSkill;
                                        repickCandidate.status = null;
                                        repickCandidate.wmsUserId = null;
                                        repickCandidate.straggled = true;
                                        repickCandidate.lastUpdate = LocalDateTime.now();
                                        repickCandidate.quantity = 0.0;

                                        try {
                                            PickSaveRequest request = new PickSaveRequest(repickCandidate, order.transactionId);
                                            wms.save(request);
                                            item.numStraggles = ++numRepicks;
                                        } catch (Exception e) {
                                            logger.info("Encountered an error attempting to repick the most recent pick" + " for Order Item {} on {} Order #{}! (primeTid: {})", item.id, orderType,
                                                    order.number, order.transactionId, e);
                                        }

                                        Label label = new Label("Repicked (In Flight)");

                                        consolidation.updateOrderItemLabel(Integer.toString(orderNumber), consolidatedItem.id, label);
                                    } else {
                                        logger.warn("Unable to auto repick Order Item {} on {} Order #{}! Item has already been repicked {} time(s). (Max # of automatic repicks {})! (primeTid: {})", item.id,
                                                orderType, order.number, numRepicks, maxAutoRepicks, order.transactionId);
                                    }
                                } else {
                                    logger.info("Found auto-repick eligible consolidateable Pick {} for Order Item {} on Order #{}. Auto-repick is disabled. (primeTid: {})",
                                            repickCandidate.id, item.id, orderType, order.number, order.transactionId);
                                }
                            } else {
                                logger.info("No need to auto-repick consolidateable Pick {} for Order Item {} on {} Order #{}. " + "(Pick Was Worked? {}, Item Released? {}, Item Placed? {}, Pick Deemed Out By Straggler? {}, Last Update: {}) (primeTid: {})",
                                        repickCandidate.id, item.id, orderType, order.number,
                                        pickWasWorked, item.released, consolidatedItem.placed, pickDeemedOut,
                                        consolidatedItem.lastUpdate, order.transactionId);
                            }
                        } else {
                            logger.warn("Unable to find a consolidated item for Order Item {} on {} Order #{} " + "(Picks for this Order Item: {}, Consolidateable Order: {}) (primeTid: {})", orderItemId,
                                    order.type, order.number, picksForItem, consolidatableOrder, order.transactionId);
                        }
                    } else {
                        logger.warn("No picks found for Item {} on {} Order #{}! (primeTid: {})", orderItemId, orderType, orderNumber, order.transactionId);
                    }
                }
            } else {
                logger.warn("Unable to find a consolidated order for {} Order #{}! (primeTid: {})", order.type,
                        order.number, order.transactionId);
            }

            boolean allItemsPlaced = true;
            boolean allItemsShipped = true;

            for (OrderItem item : order.items) {
                if (OrderItem.Status.DELETED == item.status) {
                    continue;
                }

                if (OrderItem.Status.PLACED != item.status) {
                    allItemsPlaced = false;
                    break;
                }
            }

            for (OrderItem item : order.items) {
                if (OrderItem.Status.DELETED == item.status) {
                    continue;
                }

                if (!item.shipped) {
                    allItemsShipped = false;
                    break;
                }
            }

            if (allItemsPlaced || allItemsShipped) {
                order.status = Order.Status.COMPLETE;
                order.completedOn = LocalDateTime.now();
            }

            orderService.save(order);
            ++numOrdersProcessed;
        }

        logger.info("{} exceptions handled of the {} consolidateable {} orders...", numOrdersProcessed, orders.size(), orderType);
    }
}
