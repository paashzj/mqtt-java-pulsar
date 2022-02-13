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

package com.github.shoothzj.mjp.integrate;

import com.github.shoothzj.mjp.MqsarBroker;
import com.github.shoothzj.mjp.config.MqsarConfig;
import com.github.shoothzj.mjp.config.MqttConfig;
import com.github.shoothzj.mjp.config.PulsarConfig;
import com.github.shoothzj.mjp.config.PulsarConsumeConfig;
import com.github.shoothzj.mjp.config.PulsarProduceConfig;
import com.github.shoothzj.mjp.constant.ConfigConst;
import com.github.shoothzj.mjp.util.SocketUtil;
import com.github.shoothzj.test.pulsar.TestPulsarServer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqsarTestUtil {

    public static MqsarBroker setupMqsar() throws Exception {
        MqsarConfig mqsarConfig = new MqsarConfig();
        MqttConfig mqttConfig = new MqttConfig();
        mqttConfig.setHost(ConfigConst.MQTT_HOST_DEFAULT_VALUE);
        mqttConfig.setPort(SocketUtil.getFreePort());
        mqsarConfig.setMqttConfig(mqttConfig);
        TestPulsarServer testPulsarServer = new TestPulsarServer();
        new Thread(()-> {
            try {
                testPulsarServer.start();
            } catch (Exception e) {
                log.error("test pulsar start failed ", e);
            }
        }).start();
        Thread.sleep(5000L);
        PulsarConfig pulsarConfig = new PulsarConfig();
        pulsarConfig.setHost(ConfigConst.PULSAR_HOST_DEFAULT_VALUE);
        pulsarConfig.setHttpPort(testPulsarServer.getWebPort());
        pulsarConfig.setTcpPort(testPulsarServer.getTcpPort());
        PulsarProduceConfig pulsarProduceConfig = new PulsarProduceConfig();
        pulsarProduceConfig.setDisableBatching(ConfigConst.PULSAR_PRODUCE_DISABLE_BATCHING_DEFAULT_VALUE);
        pulsarConfig.setProduceConfig(pulsarProduceConfig);
        PulsarConsumeConfig pulsarConsumeConfig = new PulsarConsumeConfig();
        pulsarConfig.setConsumeConfig(pulsarConsumeConfig);
        MqsarBroker mqsarBroker = new MqsarBroker(mqsarConfig, (username, password, clientId) -> true);
        new Thread(mqsarBroker::start).start();
        Thread.sleep(5000L);
        return mqsarBroker;
    }

}
