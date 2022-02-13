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

package com.github.shoothzj.mjp.client;

import com.github.shoothzj.mjp.constant.ConfigConst;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class Mqttv3ClientTest {

    @Test
    public void connectMqttBroker() throws MqttException {
        MqttConnectOptions connectOpt = new MqttConnectOptions();
        connectOpt.setCleanSession(true);
        connectOpt.setKeepAliveInterval(40);
        connectOpt.setUserName("admin");
        connectOpt.setPassword("123456".toCharArray());
        connectOpt.setConnectionTimeout(10);
        String url = String.format("tcp://%s:%d", ConfigConst.PULSAR_HOST_DEFAULT_VALUE, ConfigConst.MQTT_PORT_DEFAULT_VALUE);
        MqttClient mqttClient = new MqttClient(url, "clientId001");
        mqttClient.connect(connectOpt);
        Assert.assertTrue(mqttClient.isConnected());

    }
}
