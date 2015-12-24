package com.thomsonreuters.handler;

import java.io.File;
import java.util.List;

import javax.annotation.PostConstruct;

import netflix.karyon.health.HealthCheckHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.thomsonreuters.eiddo.client.EiddoClient;
import com.thomsonreuters.eiddo.client.EiddoListener;

@Singleton
public class HealthCheck implements HealthCheckHandler {
  private static final Logger log = LoggerFactory.getLogger(HealthCheck.class);

  private final EiddoClient eiddo;
  private boolean eiddoCorrupted = false;

  @Inject
  public HealthCheck(EiddoClient eiddo) {
    this.eiddo = eiddo;
    eiddo.addListener(new EiddoListener() {

      @Override
      public void onRepoChainUpdated(List<File> repoDir) {
        // TODO Auto-generated method stub

      }

      @Override
      public void onError(Throwable error, boolean fatal) {
        if (fatal) {
          eiddoCorrupted = true;
        }

      }
    });
  }

  @PostConstruct
  public void init() {
    log.info("Health check initialized.");
  }

  @Override
  public int getStatus() {
    log.info("Health check called.");
    if (eiddoCorrupted) {
      log.error("Eiddo appears to be corrupted. The instance has to be terminated and relaunched");
      return 500;
    }
    return 200;
  }
}
