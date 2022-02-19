/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.shoothzj.mjp;

import com.github.shoothzj.mjp.config.MqsarConfig;
import com.github.shoothzj.mjp.config.MqttConfig;
import com.github.shoothzj.mjp.config.PulsarConfig;
import com.github.shoothzj.mjp.module.MqttSessionKey;
import com.github.shoothzj.mjp.module.MqttTopicKey;
import com.github.shoothzj.mjp.util.ChannelUtils;
import com.github.shoothzj.mjp.util.ClosableUtils;
import com.github.shoothzj.mjp.util.MqttMessageUtil;
import com.google.common.base.Preconditions;
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
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class MqsarProcessor {

    private final MqsarServer mqsarServer;

    private final MqttConfig mqttConfig;

    private final PulsarConfig pulsarConfig;

    private final PulsarClient pulsarClient;

    private final ReentrantReadWriteLock.ReadLock rLock;

    private final ReentrantReadWriteLock.WriteLock wLock;

    private final Map<MqttSessionKey, List<MqttTopicKey>> sessionProducerMap;

    private final Map<MqttSessionKey, List<MqttTopicKey>> sessionConsumerMap;

    private final Map<MqttTopicKey, Producer<byte[]>> producerMap;

    private final Map<MqttTopicKey, Consumer<byte[]>> consumerMap;

    public MqsarProcessor(MqsarServer mqsarServer, MqsarConfig mqsarConfig) throws PulsarClientException {
        this.mqsarServer = mqsarServer;
        this.mqttConfig = mqsarConfig.getMqttConfig();
        this.pulsarConfig = mqsarConfig.getPulsarConfig();
        this.pulsarClient = PulsarClient.builder()
                .serviceUrl(String.format("pulsar://%s:%d", pulsarConfig.getHost(), pulsarConfig.getTcpPort()))
                .build();
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
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
                        new MqttFixedHeader(MqttMessageType.CONNACK,
                                false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(
                                MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION,
                                false), null);
                channel.writeAndFlush(connAckMessage);
                log.error("connection refused due to invalid protocol, client address [{}]", channel.remoteAddress());
                ctx.close();
                return;
            } else if (cause instanceof MqttIdentifierRejectedException) {
                // ineligible clientId
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK,
                                false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                                false), null);
                channel.writeAndFlush(connAckMessage);
                log.error("ineligible clientId, client address [{}]", channel.remoteAddress());
                ctx.close();
                return;
            }
            ctx.close();
            return;
        }
        String clientId = msg.payload().clientIdentifier();
        String username = msg.payload().userName();
        byte[] pwd = msg.payload().passwordInBytes();
        if (StringUtils.isBlank(clientId) || StringUtils.isBlank(username)) {
            MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.CONNACK,
                            false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                            false), null);
            channel.writeAndFlush(connAckMessage);
            log.error("the clientId username pwd cannot be empty, client address[{}]", channel.remoteAddress());
            channel.close();
            return;
        }

        if (!mqsarServer.auth(username, pwd, clientId)) {
            MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.CONNACK,
                            false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_USE_ANOTHER_SERVER,
                            false), null);
            channel.writeAndFlush(connAckMessage);
            ctx.close();
            return;
        }

        MqttSessionKey mqttSessionKey = new MqttSessionKey();
        mqttSessionKey.setClientId(clientId);
        mqttSessionKey.setUsername(username);
        ChannelUtils.setMqttSession(ctx.channel(), mqttSessionKey);
        wLock.lock();
        try {
            sessionProducerMap.put(mqttSessionKey, Lists.newArrayList());
            sessionConsumerMap.put(mqttSessionKey, Lists.newArrayList());
        } finally {
            wLock.unlock();
        }
        MqttConnAckMessage mqttConnectMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK,
                        false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false),
                null);
        channel.writeAndFlush(mqttConnectMessage);
    }

    void processPubAck(ChannelHandlerContext ctx, MqttPubAckMessage msg) {
    }

    void processPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("client address [{}]", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        if (msg.fixedHeader().qosLevel() == MqttQoS.EXACTLY_ONCE) {
            log.error("does not support QoS2 protocol. clientId [{}], username [{}] ",
                    mqttSession.getClientId(), mqttSession.getUsername());
            return;
        }

        if (msg.fixedHeader().qosLevel() == MqttQoS.FAILURE) {
            log.error("failure. clientId [{}], username [{}] ", mqttSession.getClientId(), mqttSession.getUsername());
            return;
        }

        int len = msg.payload().readableBytes();
        byte[] messageBytes = new byte[len];
        msg.payload().getBytes(msg.payload().readerIndex(), messageBytes);
        MqttSessionKey mqttSessionKey = ChannelUtils.getMqttSession(ctx.channel());
        Preconditions.checkNotNull(mqttSessionKey);
        String topic = mqsarServer.produceTopic(mqttSessionKey.getUsername(),
                mqttSessionKey.getClientId(), msg.variableHeader().topicName());
        Producer<byte[]> producer;
        producer = getOrCreateProducer(mqttSessionKey, topic);
        if (msg.fixedHeader().qosLevel() == MqttQoS.AT_MOST_ONCE) {
            producer.sendAsync(messageBytes).
                    thenAccept(messageId -> log.info("clientId [{}],"
                                    + " username [{}]. send message to pulsar success messageId: {}",
                            mqttSession.getClientId(), mqttSession.getUsername(), messageId))
                    .exceptionally((e) -> {
                        log.error("clientId [{}], username [{}]. send message to pulsar fail: ",
                                mqttSession.getClientId(), mqttSession.getUsername(), e);
                        return null;
                    });
        }
        if (msg.fixedHeader().qosLevel() == MqttQoS.AT_LEAST_ONCE) {
            try {
                MessageId messageId = producer.send(messageBytes);
                MqttPubAckMessage pubAckMessage = (MqttPubAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        MqttMessageIdVariableHeader.from(msg.variableHeader().packetId()), null);
                log.info("clientId [{}], username [{}]. send pulsar success. messageId: {}",
                        mqttSession.getClientId(), mqttSession.getUsername(), messageId);
                ctx.writeAndFlush(pubAckMessage);
            } catch (PulsarClientException e) {
                log.error("clientId [{}], username [{}]. send pulsar error: {}",
                        mqttSession.getClientId(), mqttSession.getUsername(), e.getMessage());
            }
        }
    }

    private Producer<byte[]> getOrCreateProducer(MqttSessionKey mqttSessionKey, String topic) {
        throw new UnsupportedOperationException("not implement yet");
    }

    void processPubRel(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    void processPubRec(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    void processPubComp(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    void processDisconnect(ChannelHandlerContext ctx, MqttMessage msg) {
        Channel channel = ctx.channel();
        closeMqttSession(ChannelUtils.getMqttSession(channel));
        ctx.close();
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

    private void closeMqttSession(@Nullable MqttSessionKey mqttSessionKey) {
        if (mqttSessionKey == null) {
            return;
        }
        wLock.lock();
        try {
            // find producers
            List<MqttTopicKey> produceTopicKeys = sessionProducerMap.get(mqttSessionKey);
            if (produceTopicKeys != null) {
                for (MqttTopicKey mqttTopicKey : produceTopicKeys) {
                    Producer<byte[]> producer = producerMap.get(mqttTopicKey);
                    if (producer != null) {
                        ClosableUtils.close(producer);
                    }
                }
            }
            List<MqttTopicKey> consumeTopicKeys = sessionConsumerMap.get(mqttSessionKey);
            if (consumeTopicKeys != null) {
                for (MqttTopicKey mqttTopicKey : consumeTopicKeys) {
                    Consumer<byte[]> consumer = consumerMap.get(mqttTopicKey);
                    if (consumer != null) {
                        ClosableUtils.close(consumer);
                    }
                }
            }
        } finally {
            wLock.unlock();
        }
    }
}
