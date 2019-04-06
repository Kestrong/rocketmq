package com.xjbg.rocketmq.factory;

import com.xjbg.rocketmq.listener.DefaultMqTransactionListenerImpl;
import com.xjbg.rocketmq.properties.MqConsumerProperties;
import com.xjbg.rocketmq.properties.MqProducerProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;

/**
 * @author kesc
 * @since 2019/4/3
 */
public class MqFactory {

    private static <T extends DefaultMQProducer> void setProperties(T producer, MqProducerProperties properties) {
        producer.setNamesrvAddr(properties.getNamesrvAddr());
        producer.setInstanceName(properties.getInstanceName());
        producer.setMaxMessageSize(properties.getMaxMessageSize());
        producer.setUnitMode(properties.isUnitMode());
        producer.setUnitName(properties.getUnitName());
        producer.setUseTLS(properties.isUseTLS());
        producer.setHeartbeatBrokerInterval(properties.getHeartbeatBrokerInterval());
        producer.setDefaultTopicQueueNums(properties.getDefaultTopicQueueNums());
        producer.setCompressMsgBodyOverHowmuch(properties.getCompressMsgBodyOverHowmuch());
        producer.setSendMsgTimeout(properties.getSendMsgTimeout());
        producer.setSendMessageWithVIPChannel(properties.isVipChannelEnabled());
        producer.setClientIP(properties.getClientIp());
    }

    public static DefaultMQProducer createProducer(MqProducerProperties properties) {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(properties.getProducerGroup());
        setProperties(defaultMQProducer, properties);
        return defaultMQProducer;
    }

    public static TransactionMQProducer createTransactionProducer(MqProducerProperties properties, TransactionListener transactionListener) {
        TransactionMQProducer transactionMQProducer = new TransactionMQProducer(properties.getProducerGroup());
        setProperties(transactionMQProducer, properties);
        if (transactionListener == null) {
            transactionMQProducer.setTransactionListener(new DefaultMqTransactionListenerImpl());
        }
        return transactionMQProducer;
    }

    public static DefaultMQPullConsumer createPullConsumer(MqConsumerProperties properties) {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer();
        consumer.setConsumerGroup(properties.getConsumerGroup());
        consumer.setInstanceName(properties.getInstanceName());
        consumer.setClientIP(properties.getClientIP());
        if (properties.getMaxReconsumeTimes() > 0) {
            consumer.setMaxReconsumeTimes(properties.getMaxReconsumeTimes());
        }
        if (properties.getConsumerPullTimeoutMillis() > 0) {
            consumer.setConsumerPullTimeoutMillis(properties.getConsumerPullTimeoutMillis());
        }
        consumer.setMessageModel(properties.getConsumeMessageModel());
        consumer.setUnitMode(properties.isUnitMode());
        if (StringUtils.isNotBlank(properties.getUnitName())) {
            consumer.setUnitName(properties.getUnitName());
        }
        consumer.setVipChannelEnabled(properties.isVipChannelEnabled());
        consumer.setNamesrvAddr(properties.getNamesrvAddr());
        return consumer;
    }

    public static DefaultMQPullConsumer createPullConsumer(MqConsumerProperties properties, MessageQueueListener messageQueueListener) {
        DefaultMQPullConsumer pullConsumer = createPullConsumer(properties);
        if (messageQueueListener != null) {
            pullConsumer.setMessageQueueListener(messageQueueListener);
        }
        return pullConsumer;
    }

    public static DefaultMQPushConsumer createPushConsumer(MqConsumerProperties properties) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setConsumerGroup(properties.getConsumerGroup());
        consumer.setInstanceName(properties.getInstanceName());
        consumer.setClientIP(properties.getClientIP());
        if (properties.getMaxReconsumeTimes() > 0) {
            consumer.setMaxReconsumeTimes(properties.getMaxReconsumeTimes());
        }
        if (properties.getConsumeMaxThread() > 0) {
            consumer.setConsumeThreadMax(properties.getConsumeMaxThread());
        }
        if (properties.getConsumeTimeout() > 0) {
            consumer.setConsumeTimeout(properties.getConsumeTimeout());
        }
        consumer.setMessageModel(properties.getConsumeMessageModel());
        consumer.setUnitMode(properties.isUnitMode());
        if (StringUtils.isNotBlank(properties.getUnitName())) {
            consumer.setUnitName(properties.getUnitName());
        }
        consumer.setConsumeFromWhere(properties.getConsumeFromWhere());
        consumer.setVipChannelEnabled(properties.isVipChannelEnabled());
        consumer.setNamesrvAddr(properties.getNamesrvAddr());
        consumer.setMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        return consumer;
    }

    public static DefaultMQPushConsumer createPushConsumer(MqConsumerProperties properties, MessageListener messageListener) {
        DefaultMQPushConsumer pushConsumer = createPushConsumer(properties);
        if (messageListener != null) {
            if (messageListener instanceof MessageListenerConcurrently) {
                pushConsumer.registerMessageListener((MessageListenerConcurrently) messageListener);
            } else {
                pushConsumer.registerMessageListener((MessageListenerOrderly) messageListener);
            }
        }
        return pushConsumer;
    }
}
