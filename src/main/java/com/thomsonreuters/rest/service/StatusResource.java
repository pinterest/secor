package com.thomsonreuters.rest.service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

@Path("/Status")
public class StatusResource {

  private static final Logger logger = LoggerFactory.getLogger(StatusResource.class);

  @Inject
  public StatusResource() {
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response status() {
    JSONObject response = new JSONObject();
    try {
      response.put("Status", "TODO");
      return Response.ok(response.toString()).header("Access-Control-Allow-Origin", "*").build();
    }
    catch (JSONException e) {
      logger.error("Error creating json response.", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }
}
