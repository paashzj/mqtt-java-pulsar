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

import com.github.shoothzj.mjp.module.MqttSessionKey;
import com.github.shoothzj.mjp.module.MqttTopicKey;
import com.github.shoothzj.mjp.util.MqttMessageUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class MqsarProcessor {

    private final MqsarServer mqsarServer;

    private final ReentrantReadWriteLock.ReadLock rLock;

    private final ReentrantReadWriteLock.WriteLock wLock;

    private final Map<MqttSessionKey, List<MqttTopicKey>> sessionProducerMap;

    private final Map<MqttSessionKey, List<MqttTopicKey>> sessionConsumerMap;

    private final Map<MqttTopicKey, Producer<byte[]>> producerMap;

    private final Map<MqttTopicKey, Consumer<byte[]>> consumerMap;

    public MqsarProcessor(MqsarServer mqsarServer) {
        this.mqsarServer = mqsarServer;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.sessionProducerMap = new HashMap<>();
        this.sessionConsumerMap = new HashMap<>();
        this.producerMap = new HashMap<>();
        this.consumerMap = new HashMap<>();
    }

    void processConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {
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
