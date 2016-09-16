/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools;

import java.net.URL;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Given controller host and controller port, validate the following configs:
 * <ul>
 *   <li>Table Config</li>
 *   <li>Schema</li>
 * </ul>
 * <p>This validation only checks whether the config can be successfully loaded (e.g. valid JSON format).
 * <p>No guarantee on config's content.
 */
public class ConfigValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigValidator.class);

  private static final HttpClient HTTP_CLIENT = new HttpClient();

  private final String _controllerHost;
  private final int _controllerPort;

  public ConfigValidator(String controllerHost, int controllerPort) {
    _controllerHost = controllerHost;
    _controllerPort = controllerPort;
  }

  public void validateTableConfig() {
    try {
      URL url = new URL("http", _controllerHost, _controllerPort, "/tables");
      GetMethod httpGet = new GetMethod(url.toString());
      int responseCode = HTTP_CLIENT.executeMethod(httpGet);
      String response = httpGet.getResponseBodyAsString();
      if (responseCode != 200) {
        LOGGER.error("While fetching table names, got error response code: {}, response: {}", responseCode, response);
      } else {
        JSONArray tables = new JSONObject(response).getJSONArray("tables");
        int numTables = tables.length();
        for (int i = 0; i < numTables; i++) {
          String table = tables.getString(i);
          LOGGER.info("Validating table config for table: \"{}\"", table);
          try {
            url = new URL("http", _controllerHost, _controllerPort, "/tables/" + table);
            httpGet = new GetMethod(url.toString());
            HTTP_CLIENT.executeMethod(httpGet);
            responseCode = HTTP_CLIENT.executeMethod(httpGet);
            if (responseCode != 200) {
              response = httpGet.getResponseBodyAsString();
              throw new RuntimeException("Got error response code: " + responseCode + ", response: " + response);
            }
          } catch (Exception e) {
            LOGGER.error("Table config validation failed for table: \"{}\"", table, e);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while validating table config for controller: {}, port: {}", _controllerHost,
          _controllerPort, e);
    }
  }

  public void validateSchema() {
    try {
      URL url = new URL("http", _controllerHost, _controllerPort, "/schemas");
      GetMethod httpGet = new GetMethod(url.toString());
      int responseCode = HTTP_CLIENT.executeMethod(httpGet);
      String response = httpGet.getResponseBodyAsString();
      if (responseCode != 200) {
        LOGGER.error("While fetching schema names, got error response code: {}, response: {}", responseCode, response);
      } else {
        JSONArray schemas = new JSONArray(response);
        int numSchemas = schemas.length();
        for (int i = 0; i < numSchemas; i++) {
          String schema = schemas.getString(i);
          LOGGER.info("Validating schema: \"{}\"", schema);
          try {
            url = new URL("http", _controllerHost, _controllerPort, "/schemas/" + schema);
            httpGet = new GetMethod(url.toString());
            HTTP_CLIENT.executeMethod(httpGet);
            responseCode = HTTP_CLIENT.executeMethod(httpGet);
            if (responseCode != 200) {
              response = httpGet.getResponseBodyAsString();
              throw new RuntimeException("Got error response code: " + responseCode + ", response: " + response);
            }
          } catch (Exception e) {
            LOGGER.error("Schema validation failed for schema: \"{}\"", schema, e);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while validating schema for controller: {}, port: {}", _controllerHost,
          _controllerPort, e);
    }
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("Usage: ConfigValidator <controller host> <controller port>");
      return;
    }
    String controllerHost = args[0];
    int controllerPort = Integer.parseInt(args[1]);
    System.out.println("Running config validation on host: " + controllerHost + ", port: " + controllerPort);
    ConfigValidator configValidator = new ConfigValidator(controllerHost, controllerPort);
    configValidator.validateTableConfig();
    configValidator.validateSchema();
  }
}
