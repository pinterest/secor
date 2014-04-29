/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * Qubole client encapsulates communication with a Qubole cluster.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class QuboleClient {
    private String mApiToken;

    public QuboleClient(SecorConfig config) {
        mApiToken = config.getQuboleApiToken();
    }

    private Map makeRequest(URL url, String body) throws IOException {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("X-AUTH-TOKEN", mApiToken);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accepts", "application/json");
            connection.setRequestProperty("Accept", "*/*");
            if (body != null) {
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Length",
                                              Integer.toString(body.getBytes().length));
            }
            connection.setUseCaches (false);
            connection.setDoInput(true);
            connection.setDoOutput(true);

            if (body != null) {
                // Send request.
                DataOutputStream dataOutputStream = new DataOutputStream(
                    connection.getOutputStream());
                dataOutputStream.writeBytes(body);
                dataOutputStream.flush();
                dataOutputStream.close();
            }

            // Get Response.
            InputStream inputStream = connection.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            Map response = (Map) JSONValue.parse(reader);
            if (response.get("status").equals("error")) {
                throw new RuntimeException("command " + url + " with body " + body + " failed " +
                    JSONObject.toJSONString(response));
            }
            return response;
        } catch (IOException exception) {
            if (connection != null) {
                connection.disconnect();
            }
            throw exception;
        }
    }

    private int query(String query) throws IOException {
        URL url = new URL("http://api.qubole.com/api/v1.2/commands");
        JSONObject queryJson = new JSONObject();
        queryJson.put("query", query);
        String body = queryJson.toString();
        Map response = makeRequest(url, body);
        return (Integer) response.get("id");
    }

    private void waitForCompletion(int commandId) throws IOException, InterruptedException {
        URL url = new URL("http://api.qubole.com/api/v1.2/commands/" + commandId);
        while (true) {
            Map response = makeRequest(url, null);
            if (response.get("status").equals("done")) {
                return;
            }
            System.out.println("waiting 3 seconds for results of query " + commandId +
                               ". Current status " + response.get("status"));
            Thread.sleep(3000);
        }
    }

    public void addPartition(String table, String partition) throws IOException,
            InterruptedException {
        String queryStr = "ALTER TABLE " + table + " ADD IF NOT EXISTS PARTITION (" + partition +
            ")";
        int commandId = query(queryStr);
        waitForCompletion(commandId);
    }
}
