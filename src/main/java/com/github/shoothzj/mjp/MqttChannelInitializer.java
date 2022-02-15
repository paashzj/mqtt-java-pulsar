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

import com.github.shoothzj.mjp.config.MqsarConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import org.apache.pulsar.client.api.PulsarClientException;

public class MqttChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final MqsarProcessor mqsarProcessor;

    public MqttChannelInitializer(MqsarServer mqsarServer, MqsarConfig mqsarConfig) throws PulsarClientException {
        this.mqsarProcessor = new MqsarProcessor(mqsarServer, mqsarConfig);
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        socketChannel.pipeline().addLast("decoder", new MqttDecoder(1024 * 1024));
        socketChannel.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
        socketChannel.pipeline().addLast("handler", new MqttInboundHandler(mqsarProcessor));
    }

}
