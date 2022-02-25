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

package com.github.shoothzj.mjp.metrics;

import com.github.shoothzj.mjp.MqsarBroker;
import com.github.shoothzj.mjp.integrate.MqsarTestUtil;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MetricsRegistryFactoryTest {

    private static MqsarBroker mqsarBroker;

    @BeforeAll
    public static void start() throws Exception{
        mqsarBroker = MqsarTestUtil.setupMqsar();
    }

    @Test
    public void initRegistrySuccess() throws MqttException {
        Assertions.assertNotNull(MetricsRegistryFactory.getRegistry());
    }

}
