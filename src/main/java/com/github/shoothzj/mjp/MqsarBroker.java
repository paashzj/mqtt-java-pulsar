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
import com.github.shoothzj.mjp.config.VertxConfig;
import com.github.shoothzj.mjp.util.EventLoopUtil;
import com.github.shoothzj.mjp.vertx.VertxServer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

public class MqsarBroker {

    private final MqsarConfig mqsarConfig;

    private final MqsarServer mqsarServer;

    private final EventLoopGroup acceptorGroup;

    private final EventLoopGroup workerGroup;

    public MqsarBroker(MqsarConfig mqsarConfig, MqsarServer mqsarServer) {
        this.mqsarConfig = mqsarConfig;
        this.mqsarServer = mqsarServer;
        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("mqtt-acceptor"));
        this.workerGroup = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("mqtt-worker"));
    }

    public void start() throws Exception {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptorGroup, workerGroup);
        serverBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        serverBootstrap.childHandler(new MqttChannelInitializer(mqsarServer, mqsarConfig));
        serverBootstrap.bind(mqsarConfig.getMqttConfig().getHost(), mqsarConfig.getMqttConfig().getPort());
      //  start vertx server
        VertxConfig vertxConfig = mqsarConfig.getVertxConfig();
        VertxServer vertxServer = new VertxServer();
        vertxServer.startServer(vertxConfig.getPort(), vertxConfig.getHost());
    }

    public String getMqttHost() {
        return mqsarConfig.getMqttConfig().getHost();
    }

    public int getMqttPort() {
        return mqsarConfig.getMqttConfig().getPort();
    }

    public String getPulsarHost() {
        return mqsarConfig.getPulsarConfig().getHost();
    }

    public int getPulsarHttpPort() {
        return mqsarConfig.getPulsarConfig().getHttpPort();
    }

    public int getPulsarTcpPort() {
        return mqsarConfig.getPulsarConfig().getTcpPort();
    }

}
