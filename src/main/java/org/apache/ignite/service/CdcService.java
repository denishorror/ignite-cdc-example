package org.apache.ignite.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.metric.MetricRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class CdcService {

    @Autowired
    private Ignite ignite;

    private CdcConsumer cdcConsumer;
    private final AtomicLong eventCounter = new AtomicLong(0);

    @PostConstruct
    public void init() {
        startCdcConsumer();
    }

    public void startCdcConsumer() {
        cdcConsumer = new CdcConsumer() {
            @Override
            public void start(MetricRegistry metricRegistry) {
                System.out.println("CDC Consumer started successfully with metrics");
            }

            @Override
            public boolean onEvents(Iterator<CdcEvent> events) {
                while (events.hasNext()) {
                    CdcEvent event = events.next();
                    long count = eventCounter.incrementAndGet();

                    // Базовая информация о событии
                    System.out.printf("CDC Event #%d: cacheId=%d, key=%s%n",
                            count, event.cacheId(), event.key());

                    System.out.printf("  - Value: %s, Partition: %d%n",
                            event.value(), event.partition());
                }
                return true;
            }

            @Override
            public void onTypes(Iterator<BinaryType> iterator) {

            }

            @Override
            public void onMappings(Iterator<TypeMapping> iterator) {

            }


            @Override
            public void onCacheChange(Iterator<CdcCacheEvent> iterator) {

            }

            @Override
            public void onCacheDestroy(Iterator<Integer> iterator) {

            }

            @Override
            public void stop() {
                System.out.println("CDC Consumer stopped");
            }

            @Override
            public boolean alive() {
                return CdcConsumer.super.alive();
            }
        };

        // Запускаем consumer с метриками
        cdcConsumer.start(null);
        System.out.println("CDC Service initialized");
    }

    public long getEventCount() {
        return eventCounter.get();
    }

    public IgniteCache<Integer, String> getCache(String cacheName) {
        return ignite.cache(cacheName);
    }

    public void simulateDataChanges() {
        IgniteCache<Integer, String> usersCache = getCache("users-cache");
        IgniteCache<Integer, String> ordersCache = getCache("orders-cache");

        for (int i = 0; i < 10; i++) {
            usersCache.put(i, "User-" + i);
            ordersCache.put(i, "Order-" + i);
        }

        // Некоторые изменения и удаления
        usersCache.put(1, "Updated-User-1");
        ordersCache.remove(5);
    }

    @PreDestroy
    public void shutdown() {
        if (cdcConsumer != null) {
            cdcConsumer.stop();
        }
        System.out.println("CDC Service shutdown completed");
    }
}