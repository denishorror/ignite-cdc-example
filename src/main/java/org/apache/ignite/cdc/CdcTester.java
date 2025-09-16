package org.apache.ignite.cdc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.cluster.ClusterState;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Тестер для проверки работы CDC в Ignite
 */
public class CdcTester {

    private Ignite ignite;
    private final String storagePath = Paths.get("").toAbsolutePath() + "/ignite-storage";

    public void runTest() throws Exception {
        try {
            System.out.println("=== Starting CDC Test ===");

            // Очищаем предыдущие данные
            cleanupStorage();

            // Запускаем Ignite с CDC
            startIgniteWithCdc();

            // Создаем кэш и производим операции
            testBasicOperations();

            // Тестируем различные типы операций
            testDifferentOperations();

            // Тестируем множественные события
            testMultipleEvents();

            System.out.println("=== CDC Test Completed Successfully ===");
            System.out.println("Total events captured: " + TestCdcConsumer.getEventCount());

        } finally {
            shutdown();
        }
    }

    public void startIgniteWithCdc() {
        System.out.println("Starting Ignite with CDC configuration...");

        // Конфигурация региона данных с CDC
        DataRegionConfiguration dataRegionConfig = new DataRegionConfiguration()
                .setName("cdc-test-region")
                .setInitialSize(100 * 1024 * 1024) // 100MB
                .setMaxSize(200 * 1024 * 1024)     // 200MB
                .setPersistenceEnabled(true)       // Обязательно для CDC
                .setCdcEnabled(true);              // ВКЛЮЧАЕМ CDC

        // Конфигурация хранилища
        DataStorageConfiguration storageConfig = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(dataRegionConfig)
                .setStoragePath(storagePath)
                .setWalPath(storagePath + "/wal")
                .setWalArchivePath(storagePath + "/wal-archive");

        // Общая конфигурация Ignite
        IgniteConfiguration config = new IgniteConfiguration()
                .setIgniteInstanceName("cdc-test-instance")
                .setDataStorageConfiguration(storageConfig)
                .setWorkDirectory(storagePath);

        ignite = Ignition.start(config);

        // АКТИВИРУЕМ КЛАСТЕР - это важно при использовании persistence
        ignite.cluster().state(ClusterState.ACTIVE);

        System.out.println("Ignite started successfully with CDC");
    }

    public void testBasicOperations() throws Exception {
        System.out.println("\n--- Testing Basic Operations ---");

        // Создаем кэш (CDC уже включен на уровне региона данных)
        CacheConfiguration<Integer, String> cacheConfig = new CacheConfiguration<Integer, String>()
                .setName("test-cache");

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheConfig);

        // Сбрасываем счетчик событий
        TestCdcConsumer.resetCounter();

        // Производим операции
        cache.put(1, "Value1");
        cache.put(2, "Value2");
        cache.put(3, "Value3");

        // Даем время на обработку CDC
        Thread.sleep(3000);

        System.out.println("After PUT operations: " + TestCdcConsumer.getEventCount() + " events");

        // Обновляем значение
        cache.put(1, "UpdatedValue1");
        Thread.sleep(1000);

        // Удаляем значение
        cache.remove(2);
        Thread.sleep(1000);

        System.out.println("After UPDATE/REMOVE operations: " + TestCdcConsumer.getEventCount() + " events");

        // Проверяем, что события были捕获
        if (TestCdcConsumer.getEventCount() >= 4) {
            System.out.println("✓ Basic operations test PASSED");
        } else {
            System.out.println("✗ Basic operations test FAILED");
        }
    }

    public void testDifferentOperations() throws Exception {
        System.out.println("\n--- Testing Different Operations ---");

        CacheConfiguration<String, User> userCacheConfig = new CacheConfiguration<String, User>()
                .setName("user-cache");

        IgniteCache<String, User> userCache = ignite.getOrCreateCache(userCacheConfig);

        TestCdcConsumer.resetCounter();

        // Создаем пользователей
        User user1 = new User("John", "Doe", 30);
        User user2 = new User("Jane", "Smith", 25);

        userCache.put("user1", user1);
        userCache.put("user2", user2);
        Thread.sleep(1000);

        // Обновляем пользователя
        user1.setAge(31);
        userCache.put("user1", user1);
        Thread.sleep(1000);

        // Удаляем пользователя
        userCache.remove("user2");
        Thread.sleep(1000);

        System.out.println("User operations: " + TestCdcConsumer.getEventCount() + " events");

        if (TestCdcConsumer.getEventCount() >= 4) {
            System.out.println("✓ Different operations test PASSED");
        } else {
            System.out.println("✗ Different operations test FAILED");
        }
    }

    public void testMultipleEvents() throws Exception {
        System.out.println("\n--- Testing Multiple Events ---");

        CacheConfiguration<Integer, String> massCacheConfig = new CacheConfiguration<Integer, String>()
                .setName("mass-cache");

        IgniteCache<Integer, String> massCache = ignite.getOrCreateCache(massCacheConfig);

        final int eventCount = 50;
        CountDownLatch latch = new CountDownLatch(eventCount);

        TestCdcConsumer.resetCounter();
        TestCdcConsumer.setExpectedLatch(latch);

        // Генерируем много событий
        for (int i = 0; i < eventCount; i++) {
            massCache.put(i, "MassValue_" + i);
            if (i % 10 == 0) {
                Thread.sleep(50); // Небольшая пауза
            }
        }

        // Ждем обработки всех событий
        boolean allProcessed = latch.await(10, TimeUnit.SECONDS);

        if (allProcessed) {
            System.out.println("✓ Multiple events test PASSED - " + eventCount + " events processed");
        } else {
            System.out.println("✗ Multiple events test FAILED - Only " +
                    (eventCount - latch.getCount()) + "/" + eventCount + " events processed");
        }

        // Сбрасываем защелку
        TestCdcConsumer.setExpectedLatch(null);
    }

    private void cleanupStorage() {
        File storageDir = new File(storagePath);
        if (storageDir.exists()) {
            deleteDirectory(storageDir);
        }
        storageDir.mkdirs();
        System.out.println("Storage cleaned: " + storagePath);
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    public void shutdown() {
        if (ignite != null) {
            ignite.close();
        }
        System.out.println("Resources shutdown completed");
    }

    /**
     * Test CDC Consumer для захвата событий
     */
    public static class TestCdcConsumer implements CdcConsumer {
        private static final AtomicInteger eventCounter = new AtomicInteger(0);
        private static CountDownLatch expectedLatch;

        public static void setExpectedLatch(CountDownLatch latch) {
            expectedLatch = latch;
        }

        public static void resetCounter() {
            eventCounter.set(0);
        }

        @Override
        public void start(MetricRegistry metricRegistry) {
            System.out.println("Test CDC Consumer started with metrics");
        }

        @Override
        public boolean onEvents(Iterator<CdcEvent> iterator) {
            while (iterator.hasNext()) {
                CdcEvent event = iterator.next();
                int count = eventCounter.incrementAndGet();

                System.out.printf("CDC Event #%d: cacheId=%d, key=%s%n",
                        count, event.cacheId(), event.key());
                System.out.printf("  - Value: %s, Partition: %d, Primary: %b%n",
                        event.value(), event.partition(), event.primary());

                if (expectedLatch != null) {
                    expectedLatch.countDown();
                }
            }
            return true;
        }

        public static int getEventCount() {
            return eventCounter.get();
        }

        @Override
        public void onTypes(Iterator<BinaryType> types) {
            // Пустая реализация
        }

        @Override
        public void onMappings(Iterator<TypeMapping> mappings) {
            // Пустая реализация
        }

        @Override
        public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
            // Пустая реализация
        }

        @Override
        public void onCacheDestroy(Iterator<Integer> caches) {
            // Пустая реализация
        }

        @Override
        public void stop() {
            System.out.println("Test CDC Consumer stopped");
        }
    }

    /**
     * Тестовый класс пользователя
     */
    public static class User implements Serializable {
        private String firstName;
        private String lastName;
        private int age;

        public User() {
        }

        public User(String firstName, String lastName, int age) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }

        // Getters and setters
        public String getFirstName() { return firstName; }
        public void setFirstName(String firstName) { this.firstName = firstName; }

        public String getLastName() { return lastName; }
        public void setLastName(String lastName) { this.lastName = lastName; }

        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }

        @Override
        public String toString() {
            return firstName + " " + lastName + " (" + age + ")";
        }
    }
}