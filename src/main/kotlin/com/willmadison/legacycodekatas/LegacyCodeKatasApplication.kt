package com.willmadison.legacycodekatas

import com.willmadison.legacycodekatas.fulfillment.orders.DefaultOrderService
import com.willmadison.legacycodekatas.fulfillment.warehouse.DefaultConsolidation
import com.willmadison.legacycodekatas.fulfillment.warehouse.DefaultWarehouseManagement
import com.willmadison.legacycodekatas.fulfillment.warehouse.Message
import com.willmadison.legacycodekatas.fulfillment.warehouse.exceptions.configuration.ExceptionConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling
import java.util.*

@SpringBootApplication
@EnableScheduling
class LegacyCodeKatasApplication {

    @Bean
    fun orderService() = DefaultOrderService()

    @Bean
    fun warehouseManagement() = DefaultWarehouseManagement()

    @Bean
    fun consolidation() = DefaultConsolidation()

    @Bean
    fun queue(): Queue<Message> = LinkedList<Message>()

    @Bean
    @ConfigurationProperties(prefix = "exceptions")
    fun exceptionConfiguration() = ExceptionConfiguration()
}

fun main(args: Array<String>) {
    runApplication<LegacyCodeKatasApplication>(*args)
}
