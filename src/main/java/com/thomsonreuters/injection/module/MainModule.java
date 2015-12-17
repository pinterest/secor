package com.thomsonreuters.injection.module;

import com.google.inject.AbstractModule;
import com.thomsonreuters.injection.ConsumerMainModule;

public class MainModule extends AbstractModule {
  @Override
  protected void configure() {
    // Guice bindings goes here
	  bind(ConsumerMainModule.class).asEagerSingleton();
  }
}
