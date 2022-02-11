package com.github.shoothzj.mjp;

import com.github.shoothzj.mjp.constant.ConfigConst;
import com.github.shoothzj.mjp.util.EnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
public class IPulsarClient {

    private static volatile PulsarClient pulsarClient;

    private IPulsarClient() {
    }

    public static PulsarClient getPulsarClient() {
        synchronized (IPulsarClient.class) {
            if (pulsarClient == null) {
                final String serviceUrl = String.format("%s%s:%d", "pulsar://",
                        EnvUtil.getStringVar(ConfigConst.PULSAR_HOST_PROPERTY_NAME, ConfigConst.PULSAR_HOST_ENV_NAME,
                                ConfigConst.PULSAR_HOST_DEFAULT_VALUE),
                        EnvUtil.getIntVar(ConfigConst.PULSAR_HTTP_PORT_PROPERTY_NAME, ConfigConst.PULSAR_HTTP_PORT_ENV_NAME,
                                ConfigConst.PULSAR_HTTP_PORT_DEFAULT_VALUE));
                try {
                    pulsarClient = PulsarClient.builder()
                            .serviceUrl(serviceUrl)
                            .build();
                } catch (PulsarClientException e) {
                    log.error("fail to Connect to pulsar , -{}",e.getMessage());
                }
            }
        }
        return pulsarClient;
    }
}
