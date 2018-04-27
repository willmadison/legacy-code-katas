package com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions.configuration;

import com.willmadison.legacycodekatas.fulfillment.orders.Order;

import java.util.EnumSet;
import java.util.Set;

public class ExceptionConfiguration {

    public boolean enabled;

    public boolean warehouseOperational;

    public Set<Order.Type> supportedOrderTypes = EnumSet.of(Order.Type.B2B, Order.Type.B2C, Order.Type.LARGE_BULKY_ITEM);

    public int maxAutoStraggles = 5;

    public long autoStraggleTimeframeMinutes = 45L;

    public boolean autoStraggleEnabled = false;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isWarehouseOperational() {
        return warehouseOperational;
    }

    public void setWarehouseOperational(boolean warehouseOperational) {
        this.warehouseOperational = warehouseOperational;
    }
}
