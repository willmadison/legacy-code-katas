package com.willmadison.legacycodekatas.fulfillment.warehouse.management;

public class OrderVerificationSearchRequest {

    public SearchParameters searchParameters;

    public String transactionId;

    public OrderVerificationSearchRequest(SearchParameters searchParameters, String transactionId) {
        this.searchParameters = searchParameters;
        this.transactionId = transactionId;
    }
}
