/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.shoothzj.mjp;

import com.github.shoothzj.mjp.module.MqttSessionKey;
import com.github.shoothzj.mjp.module.MqttTopicKey;
import com.github.shoothzj.mjp.util.MqttMessageUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttIdentifierRejectedException;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class MqsarProcessor {
    private final ReentrantReadWriteLock.ReadLock rLock;

    private final ReentrantReadWriteLock.WriteLock wLock;

    private final Map<MqttSessionKey, List<MqttTopicKey>> sessionProducerMap;

    private final Map<MqttSessionKey, List<MqttTopicKey>> sessionConsumerMap;

    private final Map<MqttTopicKey, Producer<byte[]>> producerMap;

    private final Map<MqttTopicKey, Consumer<byte[]>> consumerMap;

    private MqsarServer mqsarServer;

    public MqsarProcessor(MqsarServer mqsarServer) {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.mqsarServer = mqsarServer;
        this.sessionProducerMap = new HashMap<>();
        this.sessionConsumerMap = new HashMap<>();
        this.producerMap = new HashMap<>();
        this.consumerMap = new HashMap<>();
    }

    void processConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {

        Channel channel = ctx.channel();
        if (msg.decoderResult().isFailure()) {
            Throwable cause = msg.decoderResult().cause();
            if (cause instanceof MqttUnacceptableProtocolVersionException) {
                // Unsupported protocol version
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false), null);
                channel.writeAndFlush(connAckMessage);
                channel.close();
                return;
            } else if (cause instanceof MqttIdentifierRejectedException) {
                // ineligible clientId
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false), null);
                channel.writeAndFlush(connAckMessage);
                channel.close();
                return;
            }
            channel.close();
            return;
        }
        String clientId = msg.payload().clientIdentifier();
        String userName = msg.payload().userName();
        String pwd = msg.payload().password();
        if (StringUtils.isBlank(clientId) || StringUtils.isBlank(userName) || StringUtils.isBlank(pwd)) {
            MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false), null);
            channel.writeAndFlush(connAckMessage);
            channel.close();
            return;
        }

        int expire = 0;
        if (msg.variableHeader().keepAliveTimeSeconds() > 0) {
            if (channel.pipeline().names().contains("idle")) {
                channel.pipeline().remove("idle");
            }
            expire = Math.round(msg.variableHeader().keepAliveTimeSeconds() * 1.5f);
            channel.pipeline().addFirst("idle", new IdleStateHandler(0, 0, expire));
        }

        if (!mqsarServer.MqttAuth(userName,pwd,clientId)) {
            MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_USE_ANOTHER_SERVER, false), null);
            channel.writeAndFlush(connAckMessage);
            channel.close();
            return;
        }

        MqttSessionKey mqttSessionKey = new MqttSessionKey();
        mqttSessionKey.setClientId(clientId);
        mqttSessionKey.setUsername(userName);
        sessionProducerMap.put(mqttSessionKey, Lists.newArrayList());
        sessionConsumerMap.put(mqttSessionKey, Lists.newArrayList());
        MqttConnAckMessage mqttConnectMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false),
                null);
        channel.writeAndFlush(mqttConnectMessage);
    }

    void processPubAck(ChannelHandlerContext ctx, MqttPubAckMessage msg) {
    }

    void processPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
    }

    void processPubRel(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    void processPubRec(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    void processPubComp(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    void processDisconnect(ChannelHandlerContext ctx, MqttMessage msg) {

        for (Map.Entry<MqttSessionKey, List<MqttTopicKey>> sessionProducers : sessionProducerMap.entrySet()) {
            for (MqttTopicKey mqttTopicKey : sessionProducers.getValue()) {
                producerMap.remove(mqttTopicKey);
            }
            sessionProducerMap.remove(sessionProducers);
        }

        for (Map.Entry<MqttSessionKey, List<MqttTopicKey>> keyListEntry : sessionConsumerMap.entrySet()) {
            for (MqttTopicKey mqttTopicKey : keyListEntry.getValue()) {
                consumerMap.remove(mqttTopicKey);
            }
            sessionConsumerMap.remove(keyListEntry.getKey());
        }
        ctx.channel().close();
    }

    void processConnectionLost(ChannelHandlerContext ctx) {
    }

    void processSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage msg) {
    }

    void processUnSubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) {
    }

    void processPingReq(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(MqttMessageUtil.pingResp());
    }
}
