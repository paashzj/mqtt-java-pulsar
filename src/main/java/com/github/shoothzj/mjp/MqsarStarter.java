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
import com.github.shoothzj.mjp.config.MqttConfig;
import com.github.shoothzj.mjp.config.PulsarConfig;
import com.github.shoothzj.mjp.config.PulsarConsumeConfig;
import com.github.shoothzj.mjp.config.PulsarProduceConfig;
import com.github.shoothzj.mjp.constant.ConfigConst;
import com.github.shoothzj.mjp.util.EnvUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqsarStarter {

    public static void main(String[] args) {
        log.info("begin to start mqsar broker");
        MqsarConfig mqsarConfig = new MqsarConfig();
        MqttConfig mqttConfig = new MqttConfig();
        mqttConfig.setHost(EnvUtil.getStringVar(ConfigConst.MQTT_HOST_PROPERTY_NAME, ConfigConst.MQTT_HOST_ENV_NAME,
                ConfigConst.MQTT_HOST_DEFAULT_VALUE));
        mqttConfig.setPort(EnvUtil.getIntVar(ConfigConst.MQTT_PORT_PROPERTY_NAME, ConfigConst.MQTT_PORT_ENV_NAME,
                ConfigConst.MQTT_PORT_DEFAULT_VALUE));
        mqsarConfig.setMqttConfig(mqttConfig);
        PulsarConfig pulsarConfig = new PulsarConfig();
        pulsarConfig.setHost(EnvUtil.getStringVar(ConfigConst.PULSAR_HOST_PROPERTY_NAME,
                ConfigConst.PULSAR_HOST_ENV_NAME, ConfigConst.PULSAR_HOST_DEFAULT_VALUE));
        pulsarConfig.setHttpPort(EnvUtil.getIntVar(ConfigConst.PULSAR_HTTP_PORT_PROPERTY_NAME,
                ConfigConst.PULSAR_HTTP_PORT_ENV_NAME, ConfigConst.PULSAR_HTTP_PORT_DEFAULT_VALUE));
        pulsarConfig.setTcpPort(EnvUtil.getIntVar(ConfigConst.PULSAR_TCP_PORT_PROPERTY_NAME,
                ConfigConst.PULSAR_TCP_PORT_ENV_NAME, ConfigConst.PULSAR_TCP_PORT_DEFAULT_VALUE));
        PulsarProduceConfig pulsarProduceConfig = new PulsarProduceConfig();
        pulsarProduceConfig.setDisableBatching(EnvUtil.getBooleanVar(
                ConfigConst.PULSAR_PRODUCE_DISABLE_BATCHING_PROPERTY_NAME,
                ConfigConst.PULSAR_PRODUCE_DISABLE_BATCHING_ENV_NAME,
                ConfigConst.PULSAR_PRODUCE_DISABLE_BATCHING_DEFAULT_VALUE));
        pulsarProduceConfig.setMaxPendingMessages(EnvUtil.getIntVar(
                ConfigConst.PULSAR_PRODUCE_MAX_PENDING_MESSAGES_PROPERTY_NAME,
                ConfigConst.PULSAR_PRODUCE_MAX_PENDING_MESSAGES_ENV_NAME,
                ConfigConst.PULSAR_PRODUCE_MAX_PENDING_MESSAGES_DEFAULT_VALUE));
        pulsarConfig.setProduceConfig(pulsarProduceConfig);
        PulsarConsumeConfig pulsarConsumeConfig = new PulsarConsumeConfig();
        pulsarConsumeConfig.setReceiverQueueSize(EnvUtil.getIntVar(
                ConfigConst.PULSAR_CONSUME_RECEIVER_QUEUE_SIZE_PROPERTY_NAME,
                ConfigConst.PULSAR_CONSUME_RECEIVER_QUEUE_SIZE_ENV_NAME,
                ConfigConst.PULSAR_CONSUME_RECEIVER_QUEUE_SIZE_DEFAULT_VALUE
        ));
        pulsarConfig.setConsumeConfig(pulsarConsumeConfig);
        MqsarBroker mqsarBroker = new MqsarBroker(mqsarConfig);
        mqsarBroker.start();
    }

}
