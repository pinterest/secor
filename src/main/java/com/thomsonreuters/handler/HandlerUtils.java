package com.thomsonreuters.handler;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

public class HandlerUtils {

  public static String encodeUrl(String url) {
    try {
      return URLEncoder.encode(url, "UTF-8");
    }
    catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String decodeUrl(String url) {
    try {
      return URLDecoder.decode(url, "UTF-8");
    }
    catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  // need better logic, this is just temp junk
  public static String makeAbsoluteUrl(String url) {
    if (url.toLowerCase().startsWith("http://") || url.toLowerCase().startsWith("https://")) {
      return url;
    }

    return "http://" + url;
  }

  public static String getParam(HttpServerRequest<ByteBuf> request, String name) {
    List<String> values = request.getQueryParameters().get(name);
    if (values != null && !values.isEmpty()) {
      return values.get(0);
    }
    return null;
  }

  public static Map<String, String> getParamsAsMap(MultivaluedMap<String, String> query) {
    Map<String, String> params = new HashMap<>();

    for (Map.Entry<String, List<String>> entry : query.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        params.put(entry.getKey(), entry.getValue().get(0));
      }
    }
    return params;
  }

  public static Map<String, String> getParamsAsMap(HttpServerRequest<ByteBuf> request) {
    Map<String, String> params = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : request.getQueryParameters().entrySet()) {
      if (!entry.getValue().isEmpty()) {
        params.put(entry.getKey(), entry.getValue().get(0));
      }
    }
    return params;
  }

  public static String appendParameter(String url, String name, String value) {
    int hash = url.lastIndexOf('#');
    int query = url.lastIndexOf('?', hash);
    if (hash != -1) {
      String beforeHash = url.substring(0, hash);
      String afterHash = url.substring(hash);
      if (query == -1) {
        return beforeHash + "?" + name + "=" + value + afterHash;
      }
      else {
        return beforeHash + "&" + name + "=" + value + afterHash;
      }
    }
    else {
      if (query == -1) {
        return url + "?" + name + "=" + value;
      }
      else {
        return url + "&" + name + "=" + value;
      }
    }
  }
}
