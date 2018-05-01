package com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import org.springframework.util.StringUtils;

import java.io.IOException;
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

    private static final int MAX_BACKGROUND_PICK_COMPLETION_HANDLERS = 10;

    private final OrderService orderService;

    private final WarehouseManagement wms;

    private final Consolidation consolidation;

    private final Queue<Message> queue;

    private final ExceptionConfiguration configuration;

    private ObjectMapper mapper = new ObjectMapper();

    private ExecutorService backgroundExceptionHandlers = Executors.newFixedThreadPool(MAX_BACKGROUND_EXCEPTION_HANDLERS);

    private ExecutorService backgroundOrderExceptionHandlers = Executors.newFixedThreadPool(Type.values().length);

    private ExecutorService backgroundPickCompletionHandlers = Executors.newFixedThreadPool(MAX_BACKGROUND_PICK_COMPLETION_HANDLERS);

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
                logger.info("Looking up order verification status for {} Order #{} (transactionId: {})", orderType, orderNumber, order.transactionId);

                OrderVerificationSearchRequest orderVerificationSearch = new OrderVerificationSearchRequest(searchParameters, order.transactionId);
                OrderVerificationSearchResponse orderVerificationSearchResponse = wms.search(orderVerificationSearch);

                Collection<OrderVerification> orderVerifications = orderVerificationSearchResponse.verifications;

                if (!orderVerifications.isEmpty()) {
                    logger.info("Found {} order verification records for {} Order #{} (transactionId: {})", orderVerifications.size(),
                            orderType, orderNumber, order.transactionId);

                    for (OrderVerification verification : orderVerifications) {
                        if (verification.successful) {
                            orderVerificationsByOrderNumber.put(orderNumber, verification);
                            break;
                        }
                    }
                } else {
                    logger.warn("No order verifications for {} Order #{}! (transactionId: {})", orderType, orderNumber, order.transactionId);
                }
            } catch (Exception e) {
                logger.info("Encountered an exception attempting to retrieve the order verification status for {} Order #{}! (transactionId: {})",
                        orderType, orderNumber, order.transactionId, e);
            }

            try {
                logger.info("Looking up picks for {} Order #{} (transactionId: {})", orderType, orderNumber, order.transactionId);

                PickSearchRequest pickSearchRequest = new PickSearchRequest(searchParameters, order.transactionId);
                PickSearchResponse pickSearchResponse = wms.search(pickSearchRequest);

                Collection<Pick> picks = pickSearchResponse.picks;

                if (!CollectionUtils.isEmpty(picks)) {
                    logger.info("Found {} picks for {} Order #{} (transactionId: {})", picks.size(), orderType, orderNumber, order.transactionId);

                    for (Pick pick : picks) {
                        String orderItemId = pick.orderItemId;

                        if (!picksByOrderItemId.containsKey(orderItemId)) {
                            picksByOrderItemId.put(orderItemId, new ArrayList<>());
                        }

                        picksByOrderItemId.get(orderItemId).add(pick);
                    }
                } else {
                    logger.warn("No picks found for {} Order #{}! (transactionId: {})", orderType, orderNumber, order.transactionId);
                }
            } catch (Exception e) {
                logger.info("Encountered an exception attempting to retrieve the picks for {} Order #{}! (transactionId: {})",
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
                    logger.info("{} Order #{} has been scan verified but not all items have shipped. Leaving in WIP status... " + "(transactionId: {})", orderType, orderNumber, order.transactionId);
                }
            } else {
                logger.info("{} Order #{} has not completed scan verification. Checking for auto-repick candidates... (transactionId: {})", orderType, orderNumber, order.transactionId);

                int maxAutoRepicks = configuration.maxAutoStraggles;

                for (OrderItem item : order.items) {
                    if (!repickableStatuses.contains(item.status)) {
                        logger.info("Order Item {} on {} Order #{} is in {} status which is not a repickable status! Skipping! (transactionId: {})",
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
                            logger.warn("Preparing to attempt to auto repick Order Item {} on {} Order #{}! (Last Update {}) (transactionId: {}).", item.id,
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
                                        logger.info("Encountered an error attempting to re-pick the most recent pick  for Order Item {} on {} Order #{}! (transactionId: {})", item.id, orderType,
                                                order.number, order.transactionId, e);
                                    }

                                } else {
                                    logger.warn("Unable to auto repick Order Item {} on {} Order #{}! Item has already been repicked" + "{} time(s). (Max # of automatic repicks {}) (transactionId: {})!", item.id,
                                            orderType, order.number, numRepicks, maxAutoRepicks, order.transactionId);
                                }
                            } else {
                                logger.info("Found auto-repick eligible Pick {} for Order Item {} on Order #{}. Auto-repick is disabled. (transactionId: {})",
                                        mostRecentPick.id, item.id, orderType, order.number, order.transactionId);
                            }
                        } else {
                            logger.info("No need to auto-repick Pick {} for Order Item {} on {} Order #{}. " + "(Pick Was Worked? {}, Item Released? {}, Pick Deemed Out By Straggler? {}, Last Update: {}) (transactionId: {})",
                                    mostRecentPick.id, item.id, orderType, order.number,
                                    pickWasWorked, item.released, pickDeemedOut, lastUpdate, order.transactionId);
                        }
                    } else {
                        logger.warn("No picks found for Item {} on {} Order #{}! (transactionId: {})", orderItemId, orderType,
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

            logger.info("Looking up consolidation status for {} Order #{}...(transactionId: {})", orderType, orderNumber, order.transactionId);
            consolidatableOrder = consolidation.status(orderNumber, order.transactionId);

            com.willmadison.legacycodekatas.fulfillment.warehouse.management.SearchParameters searchParameters = new com.willmadison.legacycodekatas.fulfillment.warehouse.management.SearchParameters();
            searchParameters.orderNumber = orderNumber;

            Map<String, Collection<Pick>> picksByOrderItemId = new HashMap<>();

            //noinspection Duplicates
            try {
                logger.info("Looking up picks for {} Order #{} (transactionId: {})", orderType, orderNumber, order.transactionId);

                PickSearchRequest pickSearchRequest = new PickSearchRequest(searchParameters, order.transactionId);
                PickSearchResponse pickSearchResponse = wms.search(pickSearchRequest);

                Collection<Pick> picks = pickSearchResponse.picks;

                if (!CollectionUtils.isEmpty(picks)) {
                    logger.info("Found {} picks for {} Order #{} (transactionId: {})", picks.size(), orderType, orderNumber, order.transactionId);

                    for (Pick pick : picks) {
                        String orderItemId = pick.orderItemId;

                        if (!picksByOrderItemId.containsKey(orderItemId)) {
                            picksByOrderItemId.put(orderItemId, new ArrayList<>());
                        }

                        picksByOrderItemId.get(orderItemId).add(pick);
                    }
                } else {
                    logger.warn("No picks found for {} Order #{}! (transactionId: {})", orderType, orderNumber, order.transactionId);
                }
            } catch (Exception e) {
                logger.info("Encountered an exception attempting to retrieve the picks for {} Order #{}! (transactionId: {})",
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

                logger.info("Retrieved consolidateable order with {} items for {} Order #{}. Checking for auto-straggle" + " candidates... (transactionId: {})", consolidatableOrder.items.size(), order.type, orderNumber, order.transactionId);

                int maxAutoRepicks = configuration.maxAutoStraggles;

                for (OrderItem item : order.items) {
                    if (!repickableStatuses.contains(item.status)) {
                        logger.info("Order Item {} on {} Order #{} is in {} status which is not a repickable status! Skipping! (transactionId: {})",
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
                                logger.warn("Preparing to attempt to auto repick Order Item {} on {} Order #{}! (Last Update {}) (transactionId: {}).", item.id,
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
                                            logger.info("Encountered an error attempting to repick the most recent pick" + " for Order Item {} on {} Order #{}! (transactionId: {})", item.id, orderType,
                                                    order.number, order.transactionId, e);
                                        }

                                        Label label = new Label("Repicked (In Flight)");

                                        consolidation.updateOrderItemLabel(Integer.toString(orderNumber), consolidatedItem.id, label);
                                    } else {
                                        logger.warn("Unable to auto repick Order Item {} on {} Order #{}! Item has already been repicked {} time(s). (Max # of automatic repicks {})! (transactionId: {})", item.id,
                                                orderType, order.number, numRepicks, maxAutoRepicks, order.transactionId);
                                    }
                                } else {
                                    logger.info("Found auto-repick eligible consolidateable Pick {} for Order Item {} on Order #{}. Auto-repick is disabled. (transactionId: {})",
                                            repickCandidate.id, item.id, orderType, order.number, order.transactionId);
                                }
                            } else {
                                logger.info("No need to auto-repick consolidateable Pick {} for Order Item {} on {} Order #{}. " + "(Pick Was Worked? {}, Item Released? {}, Item Placed? {}, Pick Deemed Out By Straggler? {}, Last Update: {}) (transactionId: {})",
                                        repickCandidate.id, item.id, orderType, order.number,
                                        pickWasWorked, item.released, consolidatedItem.placed, pickDeemedOut,
                                        consolidatedItem.lastUpdate, order.transactionId);
                            }
                        } else {
                            logger.warn("Unable to find a consolidated item for Order Item {} on {} Order #{} " + "(Picks for this Order Item: {}, Consolidateable Order: {}) (transactionId: {})", orderItemId,
                                    order.type, order.number, picksForItem, consolidatableOrder, order.transactionId);
                        }
                    } else {
                        logger.warn("No picks found for Item {} on {} Order #{}! (transactionId: {})", orderItemId, orderType, orderNumber, order.transactionId);
                    }
                }
            } else {
                logger.warn("Unable to find a consolidated order for {} Order #{}! (transactionId: {})", order.type,
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

    @Scheduled(cron = "0 0/1 * * * *") // Every 1 minutes...
    private void checkForCompletedPicks() {
        if (configuration.enabled) {
            if (configuration.warehouseOperational) {
                logger.info("Processing completed picks...");

                Collection<Collection<Message>> messageBatches = batchPickCompletionMessages(queue);

                if (!CollectionUtils.isEmpty(messageBatches)) {
                    for (final Collection<Message> messages : messageBatches) {
                        backgroundPickCompletionHandlers.submit(() -> processCompletedPicks(messages));
                    }
                } else {
                    logger.info("No pick completion messages to process...");
                }

            } else {
                logger.info("No need to process completed picks at this moment. Warehouse is not operational!");
            }
        } else {
            logger.info("Exception handling is disabled. Unable to handle completed pick notifications!");
        }
    }

    private Collection<Collection<Message>> batchPickCompletionMessages(Collection<Message> messages) {
        Collection<Collection<Message>> messageBatches = new ArrayList<>();

        if (!CollectionUtils.isEmpty(messages)) {
            List<Message> messagesToPartition = new ArrayList<>(messages);

            int numMessages = messagesToPartition.size();
            int batchSize = numMessages / MAX_BACKGROUND_PICK_COMPLETION_HANDLERS;

            if (batchSize > 1) {
                int from = 0;
                int to = from + batchSize;

                while (from < numMessages) {
                    if (to > numMessages) {
                        to = numMessages;
                    }

                    messageBatches.add(messagesToPartition.subList(from, to));

                    from += batchSize;
                    to += batchSize;
                }
            } else {
                messageBatches.add(messages);
            }
        }

        return messageBatches;
    }

    private void processCompletedPicks(Collection<Message> pickCompletionMessages) {
        String transactionId = UUID.randomUUID().toString();

        Collection<PickCompleteNotification> pickCompleteNotifications = convertToPickCompletionNotifications(pickCompletionMessages);

        Collection<Pick> completedPicks = retrieveCompletedPicks(pickCompleteNotifications, transactionId);

        Collection<Integer> orderNumbers = new HashSet<>();
        Map<String, Collection<Pick>> picksByOrderItemId = new HashMap<>();

        if (!CollectionUtils.isEmpty(completedPicks)) {
            logger.info("Found {} completed picks! Looking up the associated orders...", completedPicks.size());

            for (Pick pick : completedPicks) {
                String orderItemId = pick.orderItemId;
                orderNumbers.add(pick.orderNumber);

                if (!picksByOrderItemId.containsKey(orderItemId)) {
                    picksByOrderItemId.put(orderItemId, new HashSet<>());
                }

                picksByOrderItemId.get(orderItemId).add(pick);
            }

            SearchParameters searchParameters = new SearchParameters();
            searchParameters.orderNumbers = orderNumbers;
            Collection<Order> orders = orderService.find(searchParameters);

            if (!CollectionUtils.isEmpty(orders)) {
                for (Order order : orders) {
                    handlePickCompletion(order, picksByOrderItemId);
                }
            } else {
                logger.warn("Found NO Cerebro Orders for the {} completed picks!", completedPicks.size());
            }
        }
    }

    private Collection<PickCompleteNotification> convertToPickCompletionNotifications(Collection<Message> pickCompletionMessages) {
        Collection<PickCompleteNotification> notifications = new ArrayList<>();

        for (Message message : pickCompletionMessages) {
            String messageBody = message.getBody();

            if (!StringUtils.isEmpty(messageBody)) {
                // Fixes Bug #1234....
                if (!messageBody.endsWith("}")) {
                    messageBody += "}";
                }

                try {
                    PickCompleteNotification notification = mapper.readValue(messageBody, PickCompleteNotification.class);
                    notifications.add(notification);
                } catch (IOException ioe) {
                    logger.error("Encountered an error attempting to parse a pick completion notification!", ioe);
                }
            }
        }

        return notifications;
    }

    private Collection<Pick> retrieveCompletedPicks(Collection<PickCompleteNotification> pickCompleteNotifications, String transactionId) {
        Collection<Pick> completedPicks = new HashSet<>();

        Collection<Integer> pickIds = new HashSet<>();

        if (!CollectionUtils.isEmpty(pickCompleteNotifications)) {
            for (PickCompleteNotification notification : pickCompleteNotifications) {
                pickIds.add(notification.getPickId());
            }
        }

        com.willmadison.legacycodekatas.fulfillment.warehouse.management.SearchParameters pickSearchParameters = new com.willmadison.legacycodekatas.fulfillment.warehouse.management.SearchParameters();
        pickSearchParameters.pickIds = pickIds;

        PickSearchRequest pickSearchRequest = new PickSearchRequest(pickSearchParameters, transactionId);
        PickSearchResponse pickSearchResponse = null;

        try {
            pickSearchResponse = wms.search(pickSearchRequest);
            completedPicks = pickSearchResponse.picks;
        } catch (Exception e) {
            logger.error("Encountered an exception attempting to search for completed picks!", e);
        }

        return completedPicks;
    }

    private void handlePickCompletion(Order order, Map<String, Collection<Pick>> picksByOrderItemId) {
        logger.info("Processing pick completion message for {} Order #{} (transactionId: {})!", order.type,
                order.number, order.transactionId);

        String reservationId = order.reservationId;

        ConsolidatableOrder consolidatedOrder = consolidation.status(order.number, order.transactionId);

        boolean isConsolidateableOrder = (!StringUtils.isEmpty(reservationId) && !reservationId.endsWith("-X")) || consolidatedOrder != null;

        for (OrderItem orderItem : order.items) {
            Collection<Pick> picks = picksByOrderItemId.get(orderItem.id);

            if (!CollectionUtils.isEmpty(picks)) {
                logger.info("Found {} picks for Order Item {} on {} Order #{} (transactionId: {})!", picks.size(),
                        orderItem.id, order.type, order.number, order.transactionId);

                Pick mostRecentPick = null;

                for (Pick pick : picks) {
                    if (mostRecentPick == null || pick.createdOn.isAfter(mostRecentPick.createdOn)) {
                        mostRecentPick = pick;
                    }
                }

                String updatedConsolidationLabel = "";
                boolean isStraggled = mostRecentPick.straggled && mostRecentPick.skill.stragglerSkill == null;
                Pick.Status pickStatus = mostRecentPick.status;

                if (isStraggled) {
                    orderItem.status = OrderItem.Status.STRAGGLED;

                    String determination = mostRecentPick.fulfillmentStatus;
                    determination = (": ".equalsIgnoreCase(determination)) ? "Unknown" : determination;
                    boolean stragglerMadeDetermination = !"Unknown".equalsIgnoreCase(determination);

                    if (!stragglerMadeDetermination) {
                        updatedConsolidationLabel = "Repick (Pending)";
                    } else {
                        logger.info("Straggler Determination for Pick {}: {} (transactionId: {})", mostRecentPick.id,
                                determination, order.transactionId);

                        boolean isPartial = determination.toLowerCase().contains("partial");
                        boolean isOut = determination.toLowerCase().contains("out");
                        boolean isCompleted = determination.toLowerCase().contains("wip");

                        if (isPartial || isOut) {
                            if (isPartial) {
                                updatedConsolidationLabel = "Partial";
                            } else {
                                updatedConsolidationLabel = "Out";
                            }

                            if (!isConsolidateableOrder) {
                                logger.warn("Straggler determined {} Order #{} was out or partially available, holding in consolidation! (transactionId: {})",
                                        order.type, order.number, order.transactionId);

                                consolidation.hold(order.number, order.transactionId);
                            }
                        } else if (isCompleted) {
                            updatedConsolidationLabel = "Repicked (Complete)";
                        } else {
                            updatedConsolidationLabel = determination;
                        }
                    }
                } else {
                    OrderItem.Status updatedStatus = orderItem.status;

                    if (pickStatus != null) {
                        switch (pickStatus) {
                            case WIP:
                            case PICKED:
                                updatedConsolidationLabel = "Picked";
                                updatedStatus = OrderItem.Status.PICKED;
                                break;
                            case ASSIGNED:
                            case DELIVERED:
                            case SUSPENDED:
                                updatedConsolidationLabel = pickStatus.getDescription();
                                break;
                        }
                    }

                    orderItem.status = updatedStatus;
                }

                if (isConsolidateableOrder && !StringUtils.isEmpty(updatedConsolidationLabel)) {
                    String orderNumber = Integer.toString(mostRecentPick.orderNumber);
                    String consolidatedItemId = Integer.toString(mostRecentPick.id);
                    Label label = new Label(updatedConsolidationLabel);

                    consolidation.updateOrderItemLabel(orderNumber, consolidatedItemId, label);
                }

                orderService.save(orderItem);
            }
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private static class PickCompleteNotification {

        private int pickId;

        private boolean straggler;

        private Map<String, Object> others = new HashMap<>();

        public int getPickId() {
            return pickId;
        }

        @JsonProperty("id")
        public void setPickId(int pickId) {
            this.pickId = pickId;
        }

        public boolean isStraggler() {
            return straggler;
        }

        public void setStraggler(boolean straggler) {
            this.straggler = straggler;
        }

        @JsonAnyGetter
        public Map<String, Object> any() {
            return others;
        }

        @JsonAnySetter
        public void set(String key, Object value) {
            others.put(key, value);
        }

        @Override
        public String toString() {
            return "PickCompleteMessage{" +
                    "pickId=" + pickId +
                    ", straggler=" + straggler +
                    ", others=" + others +
                    '}';
        }
    }
}
