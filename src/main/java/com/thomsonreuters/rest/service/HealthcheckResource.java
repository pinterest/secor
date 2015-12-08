package com.thomsonreuters.rest.service;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import netflix.karyon.health.HealthCheckHandler;

import com.google.inject.Inject;

@Api(value = "/healthcheck", description = "Health check entry point")
@Path("/healthcheck")
public class HealthcheckResource {

  private final HealthCheckHandler healthCheckHandler;

  @Inject
  public HealthcheckResource(HealthCheckHandler healthCheckHandler) {
    this.healthCheckHandler = healthCheckHandler;
  }

  @ApiOperation(value = "Health check", notes = "Returns result of the health check")
  @ApiResponses(value = { @ApiResponse(code = 200, message = "Healthy") })
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response healthcheck() {
    return Response.status(healthCheckHandler.getStatus()).build();
  }
}
