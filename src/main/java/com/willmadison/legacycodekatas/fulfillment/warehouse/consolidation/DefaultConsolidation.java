package com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation;

public class DefaultConsolidation implements Consolidation {

    @Override
    public ConsolidatableOrder status(int orderNumber, String transactionId) {
        return new ConsolidatableOrder();
    }

    @Override
    public void updateOrderItemLabel(String orderNumber, String itemId, Label label) {

    }

    @Override
    public void hold(int orderNumber, String transactionId) {

    }
}
