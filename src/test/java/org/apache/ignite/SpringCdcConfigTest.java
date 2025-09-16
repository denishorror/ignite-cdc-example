//package org.apache.ignite;
//
//import org.apache.ignite.Ignite;
//import org.apache.ignite.config.IgniteCdcConfig;
//import org.apache.ignite.configuration.DataRegionConfiguration;
//import org.apache.ignite.configuration.DataStorageConfiguration;
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.test.context.ContextConfiguration;
//import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
//
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertTrue;
//
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(classes = IgniteCdcConfig.class)
//public class SpringCdcConfigTest {
//
//    @Autowired
//    private IgniteConfiguration igniteConfiguration;
//
//    @Autowired
//    private DataStorageConfiguration dataStorageConfiguration;
//
//    @Autowired
//    private DataRegionConfiguration dataRegionConfiguration;
//
//    @Test
//    public void testConfigurationsInjected() {
//        assertNotNull("IgniteConfiguration should be injected", igniteConfiguration);
//        assertNotNull("DataStorageConfiguration should be injected", dataStorageConfiguration);
//        assertNotNull("DataRegionConfiguration should be injected", dataRegionConfiguration);
//    }
//
//    @Test
//    public void testCdcEnabled() {
//        assertTrue("CDC should be enabled", dataRegionConfiguration.isCdcEnabled());
//        assertTrue("Persistence should be enabled", dataRegionConfiguration.isPersistenceEnabled());
//    }
//
//    @Test
//    public void testCacheConfigurations() {
//        assertNotNull("Cache configurations should be set", igniteConfiguration.getCacheConfiguration());
//        assertTrue("Should have multiple cache configurations",
//                igniteConfiguration.getCacheConfiguration().length >= 2);
//    }
//}
