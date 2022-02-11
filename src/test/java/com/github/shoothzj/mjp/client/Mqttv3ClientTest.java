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
    public void connectMqttBroker(){
        MqttConnectOptions connectOpt = new MqttConnectOptions();
        connectOpt.setCleanSession(true);
        connectOpt.setKeepAliveInterval(40);
        connectOpt.setUserName("admin");
        connectOpt.setPassword("123456".toCharArray());
        connectOpt.setConnectionTimeout(10);
        String url = String.format("tcp://%s:%d", ConfigConst.PULSAR_HOST_DEFAULT_VALUE, ConfigConst.MQTT_PORT_DEFAULT_VALUE);
        try {
            MqttClient mqttClient = new MqttClient(url, "clientId001");
            mqttClient.connect(connectOpt);
            Assert.assertTrue(mqttClient.isConnected());
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
