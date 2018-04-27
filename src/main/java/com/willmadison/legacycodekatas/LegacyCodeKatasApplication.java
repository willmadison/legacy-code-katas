package com.willmadison.legacycodekatas;


import com.willmadison.legacycodekatas.fulfillment.orders.DefaultOrderService;
import com.willmadison.legacycodekatas.fulfillment.orders.OrderService;
import com.willmadison.legacycodekatas.fulfillment.warehouse.Message;
import com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation.ConsolidatableOrder;
import com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation.Consolidation;
import com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation.DefaultConsolidation;
import com.willmadison.legacycodekatas.fulfillment.warehouse.consolidation.Label;
import com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions.configuration.ExceptionConfiguration;
import com.willmadison.legacycodekatas.fulfillment.warehouse.management.DefaultWarehouseManagement;
import com.willmadison.legacycodekatas.fulfillment.warehouse.management.WarehouseManagement;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.LinkedList;
import java.util.Queue;

@SpringBootApplication
@EnableScheduling
public class LegacyCodeKatasApplication {

    @Bean
    public OrderService orderService() {
        return new DefaultOrderService();
    }

    @Bean
    public WarehouseManagement warehouseManagement() {
        return new DefaultWarehouseManagement();
    }

    @Bean
    public Consolidation consolidation() {
        return new DefaultConsolidation();
    }

    @Bean
    public Queue<Message> queue() {
        return new LinkedList<>();
    }

    @Bean
    @ConfigurationProperties(prefix = "exceptions")
    public ExceptionConfiguration exceptionConfiguration() {
        return new ExceptionConfiguration();
    }

    public static void main(String ...args) {
        SpringApplication.run(LegacyCodeKatasApplication.class, args);
    }

}
