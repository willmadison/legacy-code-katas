package com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation;

import java.time.LocalDateTime;
import java.util.Collection;

public class ConsolidatableOrder {

    public Collection<ConsolidatableOrderItem> items;

    public static class ConsolidatableOrderItem {

        public String id;

        public LocalDateTime lastUpdate;

        public boolean placed;
    }
}
