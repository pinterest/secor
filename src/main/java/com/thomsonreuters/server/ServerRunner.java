package com.thomsonreuters.server;

import java.io.IOException;

import netflix.karyon.Karyon;

import com.netflix.governator.guice.BootstrapModule;
import com.thomsonreuters.injection.BootstrapInjectionModule;

public class ServerRunner {
  public static void main(String[] args) throws IOException {

    Karyon.forApplication(BootstrapInjectionModule.class, (BootstrapModule[]) null).startAndWaitTillShutdown();
  }
}
