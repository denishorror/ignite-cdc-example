package org.apache.ignite.cdc;

import org.apache.ignite.cdc.CdcTester.TestCdcConsumer;

/**
 * Runner для запуска CDC Consumer в отдельном процессе
 */
public class CdcConsumerRunner {
    public static void main(String[] args) {
        System.out.println("Starting CDC Consumer...");

        TestCdcConsumer consumer = new TestCdcConsumer();
        consumer.start(null);

        // Бесконечный цикл для поддержания работы процесса
        try {
            while (true) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("CDC Consumer interrupted");
            consumer.stop();
        }
    }
}