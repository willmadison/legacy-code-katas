package com.willmadison.legacycodekatas.fulfillment.orders;

public class OrderItem {

    public enum Status {
        WIP,
        STRAGGLED,
        PICKED,
        DELETED,
        PLACED
    }

    public String id;

    public Status status;

    public boolean shipped;

    public boolean released;

    public int numStraggles;

}
