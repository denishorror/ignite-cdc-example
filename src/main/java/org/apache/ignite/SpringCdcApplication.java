package org.apache.ignite;

import org.apache.ignite.config.SpringAppConfig;
import org.apache.ignite.service.CdcService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class SpringCdcApplication {

    public static void main(String[] args) {
        System.out.println("Starting Spring CDC Application...");

        ApplicationContext context = SpringAppConfig.createApplicationContext();
        CdcService cdcService = context.getBean(CdcService.class);

        try {
            // Симулируем изменения данных для генерации CDC событий
            cdcService.simulateDataChanges();

            System.out.println("Data changes simulated. Waiting for CDC events...");

            // Даем время на обработку событий
            Thread.sleep(5000);

            System.out.println("Total CDC events processed: " + cdcService.getEventCount());
            System.out.println("Application completed successfully");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Spring автоматически закроет контекст и Ignite
            ((AnnotationConfigApplicationContext) context).close();
        }
    }
}
