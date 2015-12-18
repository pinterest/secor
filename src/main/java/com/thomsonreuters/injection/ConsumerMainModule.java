package com.thomsonreuters.injection;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import com.netflix.config.ConfigurationManager;
import com.pinterest.secor.main.ConsumerMain;

@Singleton
public class ConsumerMainModule {
	
	private static final Logger log = LoggerFactory.getLogger(ConsumerMainModule.class);
	
	public ConsumerMainModule() {
		log.debug("ConsumerMainModule created");
	}
	
	@PostConstruct
	public void startup() {
		String prop = ConfigurationManager.getConfigInstance().getString("secor.prop");
		log.debug("CosumerMainModule start..... "+ prop);
		System.setProperty("config", prop);
		System.setProperty("log4j.configuration","log4j.prod.properties");
		// Create the backup group
		String[] args = new String[0];
		ConsumerMain.main(args);
		
		// TODO: According to "run_consumer.sh", it needs to create another ConsumerMain for PartitionGroup
	}
	
	@PreDestroy
	public void shutdown() {
		log.debug("CosumerMainModule shutdown");
		
		// TODO: Not sure what need to do yet, may be force an upload before shutting down.
	}

}
