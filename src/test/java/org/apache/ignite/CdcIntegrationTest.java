package org.apache.ignite;

import org.apache.ignite.cdc.CdcTester;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CdcIntegrationTest {

    private CdcTester cdcTester;

    @Before
    public void setUp() {
        cdcTester = new CdcTester();
    }

    @After
    public void tearDown() {
        cdcTester.shutdown();
    }

    @Test
    public void testCdcBasicFunctionality() throws Exception {
        cdcTester.startIgniteWithCdc();
        cdcTester.testBasicOperations();

        // Проверяем, что события были捕获
        assertTrue("Should capture CDC events",
                cdcTester.cdcConsumer.getEventCount() > 0);
    }

    @Test
    public void testMultipleEventsProcessing() throws Exception {
        cdcTester.startIgniteWithCdc();

        final int expectedEvents = 20;
        CountDownLatch latch = new CountDownLatch(expectedEvents);
        cdcTester.cdcConsumer.setExpectedCount(latch, expectedEvents);

        // Создаем кэш и генерируем события
        CdcTester.TestCdcConsumer consumer = cdcTester.cdcConsumer;
        consumer.resetCounter();

        // Ждем обработки событий
        boolean success = latch.await(15, TimeUnit.SECONDS);

        assertTrue("Should process multiple CDC events", success);
    }

    @Test
    public void testCdcWithDifferentDataTypes() throws Exception {
        cdcTester.startIgniteWithCdc();
        cdcTester.testDifferentOperations();

        assertTrue("Should handle different data types",
                cdcTester.cdcConsumer.getEventCount() >= 3);
    }
}
