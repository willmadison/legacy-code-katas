package com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation;

public interface Consolidation {

    ConsolidatableOrder status(int orderNumber, String transactionId);

    void updateOrderItemLabel(String orderNumber, String itemId, Label label);

    void hold(int orderNumber, String transactionId);
}
