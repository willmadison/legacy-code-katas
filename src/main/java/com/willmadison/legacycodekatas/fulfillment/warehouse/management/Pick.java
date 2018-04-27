package com.willmadison.legacycodekatas.fulfillment.warehouse.management;

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

    public enum Status {
        SUSPENDED
    }
}
