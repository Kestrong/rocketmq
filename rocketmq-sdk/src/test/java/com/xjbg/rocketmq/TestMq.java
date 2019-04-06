package com.xjbg.rocketmq;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.IClientProfile;
import com.xjbg.rocketmq.factory.MqFactory;
import com.xjbg.rocketmq.manager.OnsManagerApi;
import com.xjbg.rocketmq.message.MessageBuilder;
import com.xjbg.rocketmq.properties.*;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

/**
 * @author kesc
 * @since 2019/4/5
 */
public class TestMq {
    private String testTopic = "local-message-center-topic";

    private MqProducerProperties mqProducerProperties() {
        MqProducerProperties properties = new MqProducerProperties();
        properties.setProducerGroup(MqConstants.DEFAULT_PRODUCER_GROUP);
        properties.setNamesrvAddr("localhost:9876");
        return properties;
    }

    private OnsProducerProperties onsProducerProperties() {
        OnsProducerProperties properties = new OnsProducerProperties();
        properties.setProducerGroup(MqConstants.DEFAULT_PRODUCER_GROUP);
        properties.setOnsProperties(onsProperties());
        return properties;
    }

    private OnsProperties onsProperties() {
        OnsProperties onsProperties = new OnsProperties();
        onsProperties.setAccessKey("accessKey");
        onsProperties.setSecretKey("secretKey");
        onsProperties.setOnsAddr("http://onsaddr-internet.aliyun.com:8080/rocketmq/nsaddr4client-internal");
        return onsProperties;
    }

    private MqConsumerProperties mqConsumerProperties() {
        MqConsumerProperties properties = new MqConsumerProperties();
        properties.setNamesrvAddr("localhost:9876");
        properties.setConsumerGroup(MqConstants.DEFAULT_CONSUMER_GROUP);
        return properties;
    }

    private OnsConsumerProperties onsConsumerProperties() {
        OnsConsumerProperties properties = new OnsConsumerProperties();
        properties.setConsumerGroup(MqConstants.DEFAULT_CONSUMER_GROUP);
        properties.setOnsProperties(onsProperties());
        return properties;
    }

    @Test
    public void testMqProducer() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = MqFactory.createProducer(mqProducerProperties());
        producer.start();
//        if (autoCreateTopic) {//for test or demo
//            producer.createTopic(producer.getCreateTopicKey(), testTopic, 4);
//        }
        producer.send(MessageBuilder.newBuilder().topic(testTopic).body("1").getRocketMQMessage(), 15000);
        producer.shutdown();
    }

    @Test
    public void testMqConsumer() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer pushConsumer = MqFactory.createPushConsumer(mqConsumerProperties(), (MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(x -> {
                System.out.println(new String(x.getBody()));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.subscribe(testTopic, "*");
        pushConsumer.start();
        Thread.sleep(20_000);
        pushConsumer.shutdown();
    }

    /**
     * I'm so poor that I could not buy a ons for test
     * so you should test it yourself and tell me if there are some bugs exist
     */
    @Test
    public void testManagerApi() {
        OnsProperties onsProperties = onsProperties();
        IAcsClient iAcsClient = OnsManagerApi.acsClient(onsProperties);
        IClientProfile profile = OnsManagerApi.getProfile(onsProperties);
        String onsRegionId = OnsManagerApi.getONSRegionId(onsProperties);
        OnsManagerApi.checkConsumer(iAcsClient, "", "");
    }

    @Test
    public void testOnsProducer() {
        Producer producer = ONSFactory.createProducer(onsProducerProperties().properties());
        producer.start();
        producer.send(MessageBuilder.newBuilder().topic(testTopic).body("1").getOnsMessage());
        producer.shutdown();
    }

    @Test
    public void testOnsConsumer() throws InterruptedException {
        Consumer consumer = ONSFactory.createConsumer(onsConsumerProperties().properties());
        consumer.subscribe(testTopic, "*", (message, context) -> {
            System.out.println(new String(message.getBody()));
            return Action.CommitMessage;
        });
        consumer.start();
        Thread.sleep(20_000);
        consumer.shutdown();
    }
}
