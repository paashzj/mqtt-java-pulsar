/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.shoothzj.mjp;

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * MQTT common in bound handler.
 */
@Sharable
@Slf4j
public class MqttInboundHandler extends ChannelInboundHandlerAdapter {

    protected final MqsarProcessor processor;

    public MqttInboundHandler(MqsarProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        Preconditions.checkArgument(message instanceof MqttMessage);
        Preconditions.checkNotNull(processor);
        MqttMessage msg = (MqttMessage) message;
        try {
            if (msg.decoderResult().isFailure()) {
                throw new IllegalStateException(msg.decoderResult().cause().getMessage());
            }
            MqttMessageType messageType = msg.fixedHeader().messageType();
            if (log.isDebugEnabled()) {
                log.debug("Processing MQTT Inbound handler message, type={}", messageType);
            }
            switch (messageType) {
                case CONNECT:
                    Preconditions.checkArgument(msg instanceof MqttConnectMessage);
                    processor.processConnect(ctx, (MqttConnectMessage) msg);
                    break;
                case SUBSCRIBE:
                    Preconditions.checkArgument(msg instanceof MqttSubscribeMessage);
                    processor.processSubscribe(ctx, (MqttSubscribeMessage) msg);
                    break;
                case UNSUBSCRIBE:
                    Preconditions.checkArgument(msg instanceof MqttUnsubscribeMessage);
                    processor.processUnSubscribe(ctx, (MqttUnsubscribeMessage) msg);
                    break;
                case PUBLISH:
                    Preconditions.checkArgument(msg instanceof MqttPublishMessage);
                    processor.processPublish(ctx, (MqttPublishMessage) msg);
                    break;
                case PUBREC:
                    processor.processPubRec(ctx, msg);
                    break;
                case PUBCOMP:
                    processor.processPubComp(ctx, msg);
                    break;
                case PUBREL:
                    processor.processPubRel(ctx, msg);
                    break;
                case DISCONNECT:
                    processor.processDisconnect(ctx, msg);
                    break;
                case PUBACK:
                    Preconditions.checkArgument(msg instanceof MqttPubAckMessage);
                    processor.processPubAck(ctx, (MqttPubAckMessage) msg);
                    break;
                case PINGREQ:
                    processor.processPingReq(ctx);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown MessageType: " + messageType);
            }
        } catch (Throwable ex) {
            ReferenceCountUtil.safeRelease(msg);
            log.error("Exception was caught while processing MQTT message, ", ex);
            ctx.close();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        processor.processConnectionLost(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn("An unexpected exception was caught while processing MQTT message. "
                + "Closing Netty channel {}.", ctx.channel(), cause);
        ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) event;
            if (e.state() == IdleState.ALL_IDLE) {
                log.warn("close connection : {} due to reached all idle time", ctx.channel());
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, event);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) {
            ctx.channel().flush();
        }
        ctx.fireChannelWritabilityChanged();
    }
}
