package com.thomsonreuters.config;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.google.inject.Inject;
import com.pinterest.secor.common.SecorConfig;

public class ServiceConfig extends SecorConfig {
	
	private final ThreadLocal<ServiceConfig> mSecorConfig = new ThreadLocal<ServiceConfig>() {

        @Override
        protected ServiceConfig initialValue() {
            // Load the default configuration file first
            Properties systemProperties = System.getProperties();
            String configProperty = systemProperties.getProperty("config");

            PropertiesConfiguration properties;
            try {
                properties = new PropertiesConfiguration(configProperty);
            } catch (ConfigurationException e) {
                throw new RuntimeException("Error loading configuration from " + configProperty);
            }

            for (final Map.Entry<Object, Object> entry : systemProperties.entrySet()) {
                properties.setProperty(entry.getKey().toString(), entry.getValue());
            }
            
            return new ServiceConfig(properties);
        }
    };

    public ServiceConfig loadFromService() throws ConfigurationException {
        return mSecorConfig.get();
    }
    
    @Inject
	public ServiceConfig(PropertiesConfiguration properties) {
		super(properties);
	}

}
