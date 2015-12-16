package com.thomsonreuters.injection;

import com.google.inject.AbstractModule;
import com.pinterest.secor.common.SecorConfig;
import com.thomsonreuters.config.ServiceConfig;

public class ConfigModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(SecorConfig.class).to(ServiceConfig.class);
		// bind(SecorConfig.class);
	}

}
