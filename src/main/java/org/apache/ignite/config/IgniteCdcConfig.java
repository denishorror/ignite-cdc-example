package org.apache.ignite.config;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IgniteCdcConfig {

    @Bean
    public DataRegionConfiguration dataRegionConfiguration() {
        return new DataRegionConfiguration()
                .setName("Default_Region")
                .setPersistenceEnabled(true)
                .setCdcEnabled(true)
                .setInitialSize(256 * 1024 * 1024) // 256MB
                .setMaxSize(512 * 1024 * 1024);    // 512MB
    }

    @Bean
    public DataStorageConfiguration dataStorageConfiguration() {
        return new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(dataRegionConfiguration())
                .setWalHistorySize(20)
                .setWalSegmentSize(64 * 1024 * 1024) // 64MB
                .setCheckpointFrequency(3 * 60 * 1000); // Checkpoint каждые 3 минуты
    }

    @Bean
    public CacheConfiguration<Integer, String> cdcCache1() {
        return new CacheConfiguration<Integer, String>()
                .setName("cdc-cache-1")
                .setCdcEnabled(true);
    }

    @Bean
    public CacheConfiguration<Integer, String> cdcCache2() {
        return new CacheConfiguration<Integer, String>()
                .setName("cdc-cache-2")
                .setCdcEnabled(true);
    }

    @Bean
    public CacheConfiguration<Integer, String> usersCache() {
        return new CacheConfiguration<Integer, String>()
                .setName("users-cache")
                .setCdcEnabled(true)
                .setStatisticsEnabled(true);
    }

    @Bean
    public CacheConfiguration<Integer, String> ordersCache() {
        return new CacheConfiguration<Integer, String>()
                .setName("orders-cache")
                .setCdcEnabled(true)
                .setStatisticsEnabled(true);
    }

    @Bean
    public IgniteConfiguration igniteConfiguration() {
        return new IgniteConfiguration()
                .setIgniteInstanceName("spring-cdc-instance")
                .setDataStorageConfiguration(dataStorageConfiguration())
                .setCacheConfiguration(
                        cdcCache1(),
                        cdcCache2(),
                        usersCache(),
                        ordersCache()
                );
    }
}