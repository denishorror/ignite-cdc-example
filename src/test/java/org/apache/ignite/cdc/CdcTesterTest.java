package org.apache.ignite.cdc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

        // Ждем некоторое время для запуска CDC Consumer вручную
        System.out.println("Please start CDC Consumer manually within 10 seconds...");
        Thread.sleep(10000);

        // Тестируем базовые операции
        cdcTester.testBasicOperations();

        // Проверяем, что события были捕获
        assertTrue("Должно быть捕获至少 4 события", CdcTester.TestCdcConsumer.getEventCount() >= 4);
    }

    @Test
    public void testDifferentOperations() throws Exception {
        // Запускаем Ignite с CDC
        cdcTester.startIgniteWithCdc();

        // Ждем некоторое время для запуска CDC Consumer вручную
        System.out.println("Please start CDC Consumer manually within 10 seconds...");
        Thread.sleep(10000);

        // Тестируем различные операции
        cdcTester.testDifferentOperations();

        // Проверяем, что события были捕获
        assertTrue("Должно быть捕获至少 4 события", CdcTester.TestCdcConsumer.getEventCount() >= 4);
    }

    @Test
    public void testMultipleEvents() throws Exception {
        // Запускаем Ignite с CDC
        cdcTester.startIgniteWithCdc();

        // Ждем некоторое время для запуска CDC Consumer вручную
        System.out.println("Please start CDC Consumer manually within 10 seconds...");
        Thread.sleep(10000);

        // Тестируем множественные события
        cdcTester.testMultipleEvents();

        // Проверяем, что все события были捕获
        assertTrue("Должно быть捕获 все 50 событий", CdcTester.TestCdcConsumer.getEventCount() >= 50);
    }
}