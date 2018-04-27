package com.willmadison.legacycodekatas.fulfillment.orders;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Order {

    public enum Status {
        NEW(1),
        REPLENISHING(2),
        READY(3),
        RESERVED(4),
        WIP(5),
        SPLIT(6),
        COMPLETE(7),
        CANCELLED(8);

        private final int statusId;

        private static final Map<Integer, Status> statusesById = new HashMap<>();

        Status(int statusId) {
            this.statusId = statusId;
            populateStatusByIdLookup();
        }

        private void populateStatusByIdLookup() {
            for (Status status : values()) {
                statusesById.put(status.statusId, status);
            }
        }

        public static Status byId(int statusId) {
            return statusesById.get(statusId);
        }
    }

    public enum Type {
        B2C(1, "Business to Customer"),
        B2B(2, "Business to Business (usually bulk)"),
        LARGE_BULKY_ITEM(3, "Large and/or oddly shaped products")
        ;

        private final int typeId;
        private final String description;

        Type(int typeId, String description) {
            this.typeId = typeId;
            this.description = description;
        }
    }

    public String id;

    public int number;

    public Status status;

    public Type type;

    public String reservationId;

    public Collection<OrderItem> items;

    public String transactionId;

    public LocalDateTime lastUpdate;

    public LocalDateTime completedOn;
}
