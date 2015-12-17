package com.thomsonreuters.injection;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.inject.Singleton;
import com.netflix.config.ConfigurationManager;

@Singleton
public class ConsumerMainModule {
	
	public ConsumerMainModule() {
		System.out.println("ConsumerMainModule created");
	}
	
	@PostConstruct
	public void startup() {
		String prop = ConfigurationManager.getConfigInstance().getString("secor.prop");
		System.out.println("CosumerMainModule start..... "+ prop);
	}
	
	@PreDestroy
	public void shutdown() {
		System.out.println("CosumerMainModule shutdown");
	}

}
