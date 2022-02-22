/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.shoothzj.mjp.constant;

public class ConfigConst {

    public static final String MQTT_HOST_PROPERTY_NAME = "mqsar.mqtt.host";

    public static final String MQTT_HOST_ENV_NAME = "MQSAR_MQTT_HOST";

    public static final String MQTT_HOST_DEFAULT_VALUE = "localhost";

    public static final String MQTT_PORT_PROPERTY_NAME = "mqsar.mqtt.port";

    public static final String MQTT_PORT_ENV_NAME = "MQSAR_MQTT_PORT";

    public static final int MQTT_PORT_DEFAULT_VALUE = 1883;

    public static final String PULSAR_HOST_PROPERTY_NAME = "mqsar.pulsar.host";

    public static final String PULSAR_HOST_ENV_NAME = "MQSAR_PULSAR_HOST";

    public static final String PULSAR_HOST_DEFAULT_VALUE = "localhost";

    public static final String PULSAR_HTTP_PORT_PROPERTY_NAME = "mqsar.pulsar.http.port";

    public static final String PULSAR_HTTP_PORT_ENV_NAME = "MQSAR_PULSAR_HTTP_PORT";

    public static final int PULSAR_HTTP_PORT_DEFAULT_VALUE = 8080;

    public static final String PULSAR_TCP_PORT_PROPERTY_NAME = "mqsar.pulsar.http.port";

    public static final String PULSAR_TCP_PORT_ENV_NAME = "MQSAR_PULSAR_TCP_PORT";

    public static final int PULSAR_TCP_PORT_DEFAULT_VALUE = 6650;

    public static final String PULSAR_PRODUCE_DISABLE_BATCHING_PROPERTY_NAME = "mqsar.pulsar.produce.disableBatching";

    public static final String PULSAR_PRODUCE_DISABLE_BATCHING_ENV_NAME = "MQSAR_PULSAR_PRODUCE_DISABLE_BATCHING";

    public static final boolean PULSAR_PRODUCE_DISABLE_BATCHING_DEFAULT_VALUE = false;

    public static final String PULSAR_PRODUCE_MAX_PENDING_MESSAGES_PROPERTY_NAME =
            "mqsar.pulsar.produce.maxPendingMessages";

    public static final String PULSAR_PRODUCE_MAX_PENDING_MESSAGES_ENV_NAME =
            "MQSAR_PULSAR_PRODUCE_MAX_PENDING_MESSAGES";

    public static final int PULSAR_PRODUCE_MAX_PENDING_MESSAGES_DEFAULT_VALUE = 1000;

    public static final String PULSAR_CONSUME_RECEIVER_QUEUE_SIZE_PROPERTY_NAME =
            "mqsar.pulsar.consume.receiverQueueSize";

    public static final String PULSAR_CONSUME_RECEIVER_QUEUE_SIZE_ENV_NAME = "MQSAR_PULSAR_CONSUME_RECEIVER_QUEUE_SIZE";

    public static final int PULSAR_CONSUME_RECEIVER_QUEUE_SIZE_DEFAULT_VALUE = 1000;

    public static final int VERTX_SERVER_DEFAULT_PORT = 20000;

    public static final String VERTX_SERVER_PORT = "mqsar.vertx.port";

    public static final String VERTX_SERVER_ENV_PORT = "VERTX_SERVER_PORT";

    public static final String VERTX_SERVER_DEFAULT_HOST = "0.0.0.0";

    public static final String VERTX_SERVER_HOST = "mqsar.vertx.host";

    public static final String VERTX_SERVER_ENV_HOST = "VERTX_SERVER_HOST";

}
