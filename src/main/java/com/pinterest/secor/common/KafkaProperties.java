package com.pinterest.secor.common;

import java.net.UnknownHostException;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

import com.pinterest.secor.util.IdUtil;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class KafkaProperties {

    private static void optionalConfig(String maybeConf, Consumer<String> configConsumer) {
        Optional.ofNullable(maybeConf).filter(conf -> !conf.isEmpty()).ifPresent(configConsumer);
    }
    
    private static Properties getCommonProperties(SecorConfig secorConfig) throws UnknownHostException {
        Properties props = new Properties();
        
        props.put("bootstrap.servers", secorConfig.getKafkaSeedBrokerHost() + ":" + secorConfig.getKafkaSeedBrokerPort());
        props.put("client.id", IdUtil.getConsumerId());
        
        optionalConfig(secorConfig.getNewConsumerRequestTimeoutMs(), conf -> props.put("request.timeout.ms", conf));
        optionalConfig(secorConfig.getSocketReceiveBufferBytes(), conf -> props.put("receive.buffer.bytes", conf));
        
        return props;
    }
    
    private static Properties getSecurityProperties(SecorConfig secorConfig) throws UnknownHostException {
        Properties props = new Properties();
        
        optionalConfig(secorConfig.getSslKeyPassword(), conf -> props.put("ssl.key.password", conf));
        optionalConfig(secorConfig.getSslKeystoreLocation(), conf -> props.put("ssl.keystore.location", conf));
        optionalConfig(secorConfig.getSslKeystorePassword(), conf -> props.put("ssl.keystore.password", conf));
        optionalConfig(secorConfig.getSslTruststoreLocation(), conf -> props.put("ssl.truststore.location", conf));
        optionalConfig(secorConfig.getSslTruststorePassword(), conf -> props.put("ssl.truststore.password", conf));
        optionalConfig(secorConfig.getSslEnabledProtocol(), conf -> props.put("ssl.enabled.protocols", conf));
        optionalConfig(secorConfig.getSslKeystoreType(), conf -> props.put("ssl.keystore.type", conf));
        optionalConfig(secorConfig.getSslProtocol(), conf -> props.put("ssl.protocol", conf));
        optionalConfig(secorConfig.getSslProvider(), conf -> props.put("ssl.provider", conf));
        optionalConfig(secorConfig.getSslTruststoreType(), conf -> props.put("ssl.truststore.type", conf));
        
        optionalConfig(secorConfig.getSaslClientCallbackHandlerClass(), conf -> props.put("sasl.client.callback.handler.class", conf));
        optionalConfig(secorConfig.getSaslJaasConfig(), conf -> props.put("sasl.jaas.config", conf));
        optionalConfig(secorConfig.getSaslKerberosServiceName(), conf -> props.put("sasl.kerberos.service.name", conf));
        optionalConfig(secorConfig.getSaslLoginCallbackHandlerClass(), conf -> props.put("sasl.login.callback.handler.class", conf));
        optionalConfig(secorConfig.getSaslLoginClass(), conf -> props.put("sasl.login.class", conf));
        optionalConfig(secorConfig.getSaslMechanism(), conf -> props.put("sasl.mechanism", conf));
        
        optionalConfig(secorConfig.getSecurityProtocol(), conf -> props.put("security.protocol", conf));
        
        return props;
    }
    
    public static Properties getConsumerProperties(SecorConfig secorConfig) throws UnknownHostException {
        Properties props = getCommonProperties(secorConfig);
        props.putAll(getSecurityProperties(secorConfig));
        
        props.put("group.id", secorConfig.getKafkaGroup());
        props.put("auto.offset.reset", secorConfig.getNewConsumerAutoOffsetReset());
        props.put("enable.auto.commit", false);
        props.put("key.deserializer", ByteArrayDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);
        
        optionalConfig(secorConfig.getMaxPollIntervalMs(), conf -> props.put("max.poll.interval.ms", conf));
        optionalConfig(secorConfig.getMaxPollRecords(), conf -> props.put("max.poll.records", conf));
        optionalConfig(secorConfig.getFetchMinBytes(), conf -> props.put("fetch.min.bytes", conf));
        optionalConfig(secorConfig.getFetchMaxBytes(), conf -> props.put("fetch.max.bytes", conf));
        optionalConfig(secorConfig.getIsolationLevel(), conf -> props.put("isolation.level", conf));
        optionalConfig(secorConfig.getNewConsumerPartitionAssignmentStrategyClass(), conf -> props.put("partition.assignment.strategy", conf));
        
        return props;
    }
    
    public static Properties getProducerProperties(SecorConfig secorConfig) throws UnknownHostException {
        Properties props = getCommonProperties(secorConfig);
        props.putAll(getSecurityProperties(secorConfig));

        props.put("key.serializer", ByteArraySerializer.class);
        props.put("value.serializer", ByteArraySerializer.class);

        return props;
    }
}
