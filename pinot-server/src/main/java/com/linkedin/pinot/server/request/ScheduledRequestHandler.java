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

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import com.linkedin.pinot.serde.SerDe;
import com.linkedin.pinot.transport.netty.NettyServer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import javax.annotation.Nullable;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScheduledRequestHandler implements NettyServer.RequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledRequestHandler.class);

  private final ServerMetrics serverMetrics;
  private final QueryScheduler queryScheduler;

  public ScheduledRequestHandler(QueryScheduler queryScheduler, ServerMetrics serverMetrics) {
    this.queryScheduler = queryScheduler;
    this.serverMetrics = serverMetrics;
  }

  @Override
  public ListenableFuture<byte[]> processRequest(ChannelHandlerContext channelHandlerContext,
      ByteBuf request) {
    final long queryStartTime = System.nanoTime();
    serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES, 1);

    LOGGER.info("Processing request : {}", request);

    byte[] byteArray = new byte[request.readableBytes()];
    request.readBytes(byteArray);
    SerDe serDe = new SerDe(new TCompactProtocol.Factory());
    final InstanceRequest instanceRequest = new InstanceRequest();

    if (! serDe.deserialize(instanceRequest, byteArray)) {
      LOGGER.error("Failed to deserialize query request from broker ip: {}",
          ((InetSocketAddress) channelHandlerContext.channel().remoteAddress()).getAddress().getHostAddress());
      DataTable result = new DataTable();
      result.addException(QueryException.INTERNAL_ERROR);
      serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
      return Futures.immediateFuture(serializeDataTable(null, serverMetrics, result, queryStartTime));
    }
    long deserializationEndTime = System.nanoTime();
    final BrokerRequest brokerRequest = instanceRequest.getQuery();
    serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.REQUEST_DESERIALIZATION, deserializationEndTime - queryStartTime);
    LOGGER.debug("Processing requestId:{},request={}", instanceRequest.getRequestId(), instanceRequest);
    final QueryRequest queryRequest = new QueryRequest(instanceRequest);
    String brokerId = instanceRequest.isSetBrokerId() ? instanceRequest.getBrokerId() :
        ((InetSocketAddress) channelHandlerContext.channel().remoteAddress()).getAddress().getHostAddress();
    // we will set the ip address as client id. This is good enough for start.
    // Ideally, broker should send it's identity as part of the request
    queryRequest.setClientId(brokerId);

    final long schedulerSubmitTime = System.nanoTime();
    ListenableFuture<DataTable> queryTask = queryScheduler.submit(queryRequest);

    // following future will provide default response in case of uncaught
    // exceptions from query processing
    ListenableFuture<DataTable> queryResponse =
        Futures.catching(queryTask, Throwable.class, new Function<Throwable, DataTable>() {
          @Nullable
          @Override
          public DataTable apply(@Nullable Throwable input) {
            // this is called iff queryTask fails with unhandled exception
            serverMetrics.addMeteredGlobalValue(ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
            DataTable result = new DataTable();
            result.addException(QueryException.INTERNAL_ERROR);
            return result;
          }
        });

    // transform the DataTable to serialized byte[] to send back to broker
    ListenableFuture<byte[]> serializedQueryResponse = Futures.transform(queryResponse, new Function<DataTable, byte[]>() {
      @Nullable
      @Override
      public byte[] apply(@Nullable DataTable instanceResponse) {
        long totalNanos = System.nanoTime() - schedulerSubmitTime;
        long serializationStartTime = System.nanoTime();
        serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.QUERY_PROCESSING, totalNanos);
        if (instanceResponse == null) {
          LOGGER.warn("Instance response is null for requestId: {}, brokerId: {}", instanceRequest.getRequestId(),
              instanceRequest.getBrokerId());
          return new byte[0];
        }
        byte[] responseBytes;
        try {
          responseBytes = instanceResponse.toBytes();
        } catch (Exception e) {
          serverMetrics.addMeteredGlobalValue(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
          LOGGER.error("Got exception while serializing response for requestId: {}, brokerId: {}",
              instanceRequest.getRequestId(), instanceRequest.getBrokerId(), e);
          responseBytes = null;
        }
        long serializationEndTime = System.nanoTime();
        serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.RESPONSE_SERIALIZATION,
            serializationEndTime - serializationStartTime);
        serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.TOTAL_QUERY_TIME,
            serializationEndTime - queryStartTime);

        return responseBytes;
      }
    });

    return serializedQueryResponse;
  }

  static byte[] serializeDataTable(InstanceRequest instanceRequest,
      ServerMetrics metrics,
      DataTable instanceResponse, long queryStartTime) {
    byte[] responseByte;
    long serializationStartTime = System.nanoTime();
    try {
      if (instanceResponse == null) {
        LOGGER.warn("Instance response is null for requestId: {}, brokerId: {}",
            instanceRequest.getRequestId(), instanceRequest.getBrokerId());
        responseByte = new byte[0];
      } else {
        responseByte = instanceResponse.toBytes();
      }
    } catch (Exception e) {
      metrics.addMeteredGlobalValue(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
      LOGGER.error("Got exception while serializing response for requestId: {}, brokerId: {}",
          instanceRequest.getRequestId(), instanceRequest.getBrokerId(), e);
      responseByte = null;
    }
    long serializationEndTime = System.nanoTime();
    metrics.addPhaseTiming(instanceRequest.getQuery(), ServerQueryPhase.RESPONSE_SERIALIZATION,
        serializationEndTime - serializationStartTime);
    metrics.addPhaseTiming(instanceRequest.getQuery(), ServerQueryPhase.TOTAL_QUERY_TIME,
        serializationEndTime - queryStartTime);
    return responseByte;
  }
}
