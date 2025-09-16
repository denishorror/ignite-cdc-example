package org.apache.ignite;

import org.apache.ignite.cdc.CdcTester;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * JUnit тест для проверки работы CDC в Ignite
 */
public class CdcTesterTest {

    private CdcTester cdcTester;

    @Before
    public void setUp() {
        cdcTester = new CdcTester();
    }

    @After
    public void tearDown() {
        if (cdcTester != null) {
            cdcTester.shutdown();
        }
    }

    @Test
    public void testBasicOperations() throws Exception {
        // Запускаем Ignite с CDC
        cdcTester.startIgniteWithCdc();

        // Запускаем CDC Consumer вручную (в реальном сценарии он запускался бы через CdcMain)
        CdcTester.TestCdcConsumer consumer = new CdcTester.TestCdcConsumer();
        consumer.start(null);

        // Тестируем базовые операции
        cdcTester.testBasicOperations();

        // Проверяем, что события были捕获
        assertTrue("Должно быть 4 события", CdcTester.TestCdcConsumer.getEventCount() >= 4);
    }

    @Test
    public void testDifferentOperations() throws Exception {
        // Запускаем Ignite с CDC
        cdcTester.startIgniteWithCdc();

        // Запускаем CDC Consumer вручную
        CdcTester.TestCdcConsumer consumer = new CdcTester.TestCdcConsumer();
        consumer.start(null);

        // Тестируем различные операции
        cdcTester.testDifferentOperations();

        // Проверяем, что события были捕获
        assertTrue("Должно быть 4 события", CdcTester.TestCdcConsumer.getEventCount() >= 4);
    }

    @Test
    public void testMultipleEvents() throws Exception {
        // Запускаем Ignite с CDC
        cdcTester.startIgniteWithCdc();

        // Запускаем CDC Consumer вручную
        CdcTester.TestCdcConsumer consumer = new CdcTester.TestCdcConsumer();
        consumer.start(null);

        // Тестируем множественные события
        cdcTester.testMultipleEvents();

        // Проверяем, что все события были捕获
        assertTrue("Должно быть все 50 событий", CdcTester.TestCdcConsumer.getEventCount() >= 50);
    }

    @Test
    public void testCdcConsumerDirectly() throws Exception {
        // Создаем защелку для 5 событий
        CountDownLatch latch = new CountDownLatch(5);
        CdcTester.TestCdcConsumer.setExpectedLatch(latch);
        CdcTester.TestCdcConsumer.resetCounter();

        // Запускаем Ignite с CDC
        cdcTester.startIgniteWithCdc();

        // Запускаем CDC Consumer вручную
        CdcTester.TestCdcConsumer consumer = new CdcTester.TestCdcConsumer();
        consumer.start(null);

        // Создаем кэш и производим операции
        cdcTester.testBasicOperations();

        // Ждем обработки всех событий
        boolean allProcessed = latch.await(10, TimeUnit.SECONDS);

        // Проверяем, что все события были捕获
        assertTrue("Должно быть捕获 все 5 событий", allProcessed);
        assertTrue("Должно быть捕获至少 5 событий", CdcTester.TestCdcConsumer.getEventCount() >= 5);
    }
}