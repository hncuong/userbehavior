package utilities;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Tung NGUYEN-DUY, <tungnd.ptit@gmail.com>
 * @date 22/04/2016
 * <p>
 * Creates a singleton SystemInfo to load properties of project.
 */
public class SystemInfo {

    private static SystemInfo systemInfo = new SystemInfo();
    static {
        try {
            PropertyConfigurator.configure("log4j.properties");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Logger
     */
    private static final Logger logger = LoggerFactory.getLogger(SystemInfo.class);
    /**
     * File path of configuration
     */
    private static String configFilePath = "conf.properties";
    /**
     * A configuration
     * It be singleton pattern
     */
    private static Configuration configuration;

    /**
     * Use static methods only.
     */
    private SystemInfo() {
        try {
            PropertyConfigurator.configure("log4j.properties");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Get single Configuration
     *
     * @return
     */

    public static Configuration getConfiguration() {
        if (configuration == null) {
            try {
                PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration(configFilePath);
                FileChangedReloadingStrategy reloadingStrategy = new FileChangedReloadingStrategy();
                propertiesConfiguration.setReloadingStrategy(reloadingStrategy);
                configuration = propertiesConfiguration;
            } catch (ConfigurationException e) {
                e.printStackTrace();
                logger.error(e.toString());
            }
        }
        return configuration;
    }

}