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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
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
                log.error("connection refused due to invalid protocol, client address[{}]", channel.remoteAddress());
                channel.close();
                return;
            } else if (cause instanceof MqttIdentifierRejectedException) {
                // ineligible clientId
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK,
                                false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                                false), null);
                channel.writeAndFlush(connAckMessage);
                log.error("ineligible clientId, client address[{}]", channel.remoteAddress());
                channel.close();
                return;
            }
            channel.close();
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
            channel.close();
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
        ChannelFuture future = channel.writeAndFlush(mqttConnectMessage);
    }

    void processPubAck(ChannelHandlerContext ctx, MqttPubAckMessage msg) {
    }

    void processPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) throws PulsarClientException, InterruptedException {

        if (msg.fixedHeader().qosLevel() == MqttQoS.EXACTLY_ONCE) {
            log.error("Does not support to QoS2 protocol");
            return;
        }

        if (msg.fixedHeader().qosLevel() == MqttQoS.FAILURE) {
            log.error("FAILURE");
            return;
        }

        int len = msg.payload().readableBytes();
        byte[] messageBytes = new byte[len];

        msg.payload().getBytes(msg.payload().readerIndex(), messageBytes);
        MqttSessionKey mqttSessionKey = ChannelUtils.getMqttSession(ctx.channel());
        Preconditions.checkNotNull(mqttSessionKey);
        String topic = mqsarServer.produceTopic(mqttSessionKey.getUsername(),
                mqttSessionKey.getClientId(), msg.variableHeader().topicName());
        Producer<byte[]> producer = getOrCreateProducer(mqttSessionKey, topic);
        if (msg.fixedHeader().qosLevel() == MqttQoS.AT_MOST_ONCE) {
            producer.sendAsync(messageBytes).
                    thenAccept(messageId -> log.info("send message to pulsar success messageId {}", messageId))
                    .exceptionally((e) ->{
                        log.error("send message to pulsar fail : {}",e.getMessage());
                        return null;
                    });
        }
        if (msg.fixedHeader().qosLevel() == MqttQoS.AT_LEAST_ONCE) {
            try {
                MessageId messageId = producer.send(messageBytes);
                MqttPubAckMessage pubAckMessage = (MqttPubAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        MqttMessageIdVariableHeader.from(msg.variableHeader().packetId()), null);
                log.info("Send pulsar success. messageId :{}", messageId);
                ChannelFuture future = ctx.writeAndFlush(pubAckMessage);
                future.addListener(
                        (ChannelFutureListener) channelFuture -> {
                            if (channelFuture.isSuccess()) {
                                log.info("send message to mqtt client success ");
                            } else {
                                log.error("send message to mqtt client fail");
                            }
                        }
                );

            } catch (PulsarClientException e) {
                log.error("Send pulsar error : {}", e.getMessage());
            }
        }

    }

    private Producer<byte[]> getOrCreateProducer(MqttSessionKey mqttSessionKey, String topic) throws PulsarClientException {

        List<MqttTopicKey> mqttTopicKeys = sessionProducerMap.get(mqttSessionKey);
        if (mqttTopicKeys.size() != 0) {
            for (MqttTopicKey mqttTopicKey : mqttTopicKeys) {
                Producer<byte[]> producer = producerMap.get(mqttTopicKey);
                if (producer == null && topic.equals(mqttTopicKey.getTopic())) {
                    producer = pulsarClient.newProducer()
                            .topic(mqttTopicKey.getTopic())
                            .create();
                    wLock.lock();
                    producerMap.put(mqttTopicKey, producer);
                    wLock.unlock();
                    log.info("begin to create producer. mqttTopic {}, topic {}", topic, topic);
                    return producer;
                }
            }
        }
        MqttTopicKey mqttTopicKey = new MqttTopicKey();
        mqttTopicKey.setMqttSessionKey(mqttSessionKey);
        mqttTopicKey.setTopic(topic);
        ArrayList<MqttTopicKey> topics = Lists.newArrayList();
        topics.add(mqttTopicKey);
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
        wLock.lock();
        sessionProducerMap.put(mqttSessionKey, topics);
        producerMap.put(mqttTopicKey, producer);
        wLock.unlock();
        log.info("begin to create producer. mqttTopic {}, topic {}", topic, topic);
        return producer;
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
        channel.close();
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
