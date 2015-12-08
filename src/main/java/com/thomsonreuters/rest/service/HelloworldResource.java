package com.thomsonreuters.rest.service;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.config.ConfigurationManager;
import com.netflix.governator.annotations.Configuration;

@Singleton
@Path("/hello")
@Api(value = "/hello")
public class HelloworldResource {

  private static final Logger logger = LoggerFactory.getLogger(HelloworldResource.class);

  @Configuration("1p.service.name")
  private Supplier<String> appName = Suppliers.ofInstance("One Platform");

  @Inject
  public HelloworldResource() {

  }

  @ApiOperation(value = "get to name", notes = "return name")
  @ApiResponses(value = { @ApiResponse(code = 200, message = "success"), @ApiResponse(code = 500, message = "error") })
  @Path("to/{name}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response helloTo(@PathParam("name") String name) {
    JSONObject response = new JSONObject();
    try {
      logger.info("Hello " + name + " from " + appName.get());

      response.put("Message", "Hello " + name + " from " + appName.get());
      return Response.ok(response.toString()).header("Access-Control-Allow-Origin", "*").build();
    }
    catch (JSONException e) {
      logger.error("Error creating json response.", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  @Path("to/person")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response helloToPerson(String name) {
    JSONObject response = new JSONObject();
    try {
      response.put("Message", "Hello " + name + " from " + appName.get());
      return Response.ok(response.toString()).header("Access-Control-Allow-Origin", "*").build();
    }
    catch (JSONException e) {
      logger.error("Error creating json response.", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response hello() {
    JSONObject response = new JSONObject();
    try {

      logger.info("Hello from " + appName.get() + " running on instance "
          + ConfigurationManager.getDeploymentContext().getDeploymentServerId());

      response.put("Message", "Hello from " + appName.get() + " running on instance "
          + ConfigurationManager.getDeploymentContext().getDeploymentServerId());
      return Response.ok(response.toString()).header("Access-Control-Allow-Origin", "*").build();
    }
    catch (JSONException e) {
      logger.error("Error creating json response.", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }
}
