package org.apache.ignite.config;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "org.apache.ignite")
public class SpringAppConfig {

    @Bean
    public ApplicationContext applicationContext() {
        return new AnnotationConfigApplicationContext(SpringAppConfig.class);
    }

    @Bean(destroyMethod = "close")
    public Ignite ignite(IgniteConfiguration igniteConfiguration) {
        return Ignition.start(igniteConfiguration);
    }

    public static ApplicationContext createApplicationContext() {
        return new AnnotationConfigApplicationContext(SpringAppConfig.class);
    }
}