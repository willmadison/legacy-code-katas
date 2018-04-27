package com.willmadison.legacycodekatas.fulfillment.warehouse.management;

public class PickSaveRequest {

    public Pick pick;

    public String transactionId;

    public PickSaveRequest(Pick pick, String transactionId) {
        this.pick = pick;
        this.transactionId = transactionId;
    }
}
