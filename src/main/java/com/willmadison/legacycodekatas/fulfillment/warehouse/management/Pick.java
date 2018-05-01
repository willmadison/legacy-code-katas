package com.willmadison.legacycodekatas.fulfillment.warehouse.management;

import java.time.Instant;
import java.time.LocalDateTime;

public class Pick {

    public int id;
    public String orderItemId;
    public LocalDateTime lastUpdate;
    public Status status;
    public String wmsUserId;
    public boolean straggled;
    public Skill skill;

    public Double quantity;
    public Integer orderNumber;
    public LocalDateTime createdOn;
    public String fulfillmentStatus;

    public enum Status {
        SUSPENDED("Suspended"),
        WIP("Work in Progress"),
        PICKED("Successfully Picked"),
        ASSIGNED("Assigned to Picker"),
        DELIVERED("Delivered to Picker");

        private final String description;

        Status(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }
}
