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
package com.linkedin.pinot.server.request;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.serde.SerDe;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simple implementation of RequestHandler.
 */
public class SimpleRequestHandler implements RequestHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleRequestHandler.class);

  private ServerMetrics _serverMetrics;
  QueryExecutor _queryExecutor = null;

  public SimpleRequestHandler(QueryExecutor queryExecutor, ServerMetrics serverMetrics) {
    _queryExecutor = queryExecutor;
    _serverMetrics = serverMetrics;
  }

  @Override
  public byte[] processRequest(ChannelHandlerContext channelHandlerContext, ByteBuf request) {

    long queryStartTime = System.nanoTime();
    _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES, 1);

    LOGGER.debug("processing request : {}", request);

    DataTable instanceResponse = null;

    byte[] byteArray = new byte[request.readableBytes()];
    request.readBytes(byteArray);
    SerDe serDe = new SerDe(new TCompactProtocol.Factory());
    BrokerRequest brokerRequest = null;
    try {
      final InstanceRequest instanceRequest = new InstanceRequest();
      if (! serDe.deserialize(instanceRequest, byteArray) ) {
        // the deserialize method logs and suppresses exceptionx
        LOGGER.error("Failed to deserialize query request");
        DataTable result = new DataTable();
        result.addException(QueryException.INTERNAL_ERROR);
        _serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
        return serializeDataTable(brokerRequest, instanceResponse, queryStartTime);
      }
      long deserRequestTime = System.nanoTime();
      brokerRequest = instanceRequest.getQuery();
      _serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.REQUEST_DESERIALIZATION, deserRequestTime - queryStartTime);
      LOGGER.debug("Processing requestId:{},request={}", instanceRequest.getRequestId(), instanceRequest);

      QueryRequest queryRequest = new QueryRequest(instanceRequest);
      String brokerId = instanceRequest.isSetBrokerId() ? instanceRequest.getBrokerId() :
          ((InetSocketAddress) channelHandlerContext.channel().remoteAddress()).getAddress().getHostAddress();
      // we will set the ip address as client id. This is good enough for start.
      // Ideally, broker should send it's identity as part of the request
      queryRequest.setClientId(brokerId);

      long startTime = System.nanoTime();
      instanceResponse = _queryExecutor.processQuery(queryRequest);
      long totalNanos = System.nanoTime() - startTime;
      _serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.QUERY_PROCESSING, totalNanos);
    } catch (Exception e) {
      LOGGER.error("Got exception while processing request. Returning error response", e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
      DataTableBuilder dataTableBuilder = new DataTableBuilder(null);
      List<ProcessingException> exceptions = new ArrayList<ProcessingException>();
      ProcessingException exception = QueryException.INTERNAL_ERROR.deepCopy();
      exception.setMessage(e.getMessage());
      exceptions.add(exception);
      instanceResponse = dataTableBuilder.buildExceptions();
    }
    return serializeDataTable(brokerRequest, instanceResponse, queryStartTime);
  }

  private byte[] serializeDataTable(BrokerRequest brokerRequest, DataTable instanceResponse, long queryStartTime) {
    byte[] responseByte;
    long serializationStartTime = System.nanoTime();
    try {
      if (instanceResponse == null) {
        LOGGER.warn("Instance response is null.");
        responseByte = new byte[0];
      } else {
        responseByte = instanceResponse.toBytes();
      }
    } catch (Exception e) {
      _serverMetrics.addMeteredGlobalValue(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
      LOGGER.error("Got exception while serializing response.", e);
      responseByte = null;
    }
    long serializationEndTime = System.nanoTime();
    _serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.RESPONSE_SERIALIZATION, serializationEndTime - serializationStartTime);
    _serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.TOTAL_QUERY_TIME, serializationEndTime - queryStartTime);
    return responseByte;
  }
}
