package com.thomsonreuters.config;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.configuration.ConfigurationException;

import com.google.inject.Inject;
import com.pinterest.secor.common.SecorConfig;

public class ConfigurationService {
	
	private SecorConfig config;
	
	@Inject
	public ConfigurationService(SecorConfig config) {
		try {
			this.config = config.loadFromService();
		} catch (OperationNotSupportedException | ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public SecorConfig getConfig() {
		return config;
	}
	
}
