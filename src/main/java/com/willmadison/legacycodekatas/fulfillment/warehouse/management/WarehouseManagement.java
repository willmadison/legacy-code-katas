package com.willmadison.legacycodekatas.fulfillment.warehouse.management;

public interface WarehouseManagement {

    OrderVerificationSearchResponse search(OrderVerificationSearchRequest request);

    PickSearchResponse search(PickSearchRequest reqeust);

    PickSaveResponse save(PickSaveRequest request);
}
