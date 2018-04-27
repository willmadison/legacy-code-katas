package com.willmadison.legacycodekatas.fulfillment.warehouse.management;

public class PickSearchRequest {

    public SearchParameters searchParameters;

    public String transactionId;

    public PickSearchRequest(SearchParameters searchParameters, String transactionId) {
        this.searchParameters = searchParameters;
        this.transactionId = transactionId;
    }
}
