package com.xjbg.rocketmq.container;

import com.xjbg.rocketmq.enums.ConsumerType;
import com.xjbg.rocketmq.factory.MqFactory;
import com.xjbg.rocketmq.properties.MqConsumerProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kesc
 * @since 2019/4/4
 */
@Slf4j
@Getter
public class MqConsumerContainer extends AbstractConsumerContainer<MqConsumerProperties> {
    /**
     * groupName:consumer
     */
    private final ConcurrentHashMap<String, MQPullConsumerScheduleService> pullConsumersMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, DefaultMQPushConsumer> pushConsumersMap = new ConcurrentHashMap<>();


    private void start(MQPullConsumerScheduleService pullConsumer) {
        try {
            pullConsumer.start();
        } catch (MQClientException e) {
            log.error("rocketmq consumer start fail", e);
        }
    }

    private void start(MQPushConsumer pullConsumer) {
        try {
            pullConsumer.start();
        } catch (MQClientException e) {
            log.error("rocketmq consumer start fail", e);
        }
    }

    @Override
    public void doStartAll() {
        for (Map.Entry<String, MQPullConsumerScheduleService> entry : pullConsumersMap.entrySet()) {
            start(entry.getValue());
            log.info("start rocketmq consumer:{}", entry.getKey());
        }
        for (Map.Entry<String, DefaultMQPushConsumer> entry : pushConsumersMap.entrySet()) {
            start(entry.getValue());
            log.info("start rocketmq consumer:{}", entry.getKey());
        }
    }


    @Override
    public void doShutdownAll() {
        for (Map.Entry<String, MQPullConsumerScheduleService> entry : pullConsumersMap.entrySet()) {
            entry.getValue().shutdown();
            log.info("shutdown rocketmq consumer:{}", entry.getKey());
        }
        for (Map.Entry<String, DefaultMQPushConsumer> entry : pushConsumersMap.entrySet()) {
            entry.getValue().shutdown();
            log.info("shutdown rocketmq consumer:{}", entry.getKey());
        }
    }


    @Override
    public void doRegisterConsumer(final MqConsumerProperties properties, final ConsumerType consumerType,final Object messageListener) {
        if (pushConsumersMap.containsKey(properties.getConsumerGroup()) || pullConsumersMap.containsKey(properties.getConsumerGroup())) {
            log.info("consumer group [{}] already exist", properties.getConsumerGroup());
            return;
        }
        switch (consumerType) {
            case PULL: {
                DefaultMQPullConsumer pullConsumer = MqFactory.createPullConsumer(properties);
                MQPullConsumerScheduleService pullConsumerScheduleService = new MQPullConsumerScheduleService(properties.getConsumerGroup());
                pullConsumerScheduleService.setDefaultMQPullConsumer(pullConsumer);
                if (properties.getConsumeMaxThread() > 0) {
                    pullConsumerScheduleService.setPullThreadNums(properties.getConsumeMaxThread());
                }
                pullConsumerScheduleService.registerPullTaskCallback(properties.getTopic(), (PullTaskCallback) messageListener);
                pullConsumersMap.put(properties.getConsumerGroup(), pullConsumerScheduleService);
                if (isStarted) {
                    start(pullConsumerScheduleService);
                }
                break;
            }
            default: {
                DefaultMQPushConsumer pushConsumer = MqFactory.createPushConsumer(properties);
                if (messageListener != null) {
                    if (messageListener instanceof MessageListenerConcurrently) {
                        pushConsumer.registerMessageListener((MessageListenerConcurrently) messageListener);
                    } else {
                        pushConsumer.registerMessageListener((MessageListenerOrderly) messageListener);
                    }
                }
                try {
                    pushConsumer.subscribe(properties.getTopic(), properties.getExpression());
                    pushConsumersMap.put(properties.getConsumerGroup(), pushConsumer);
                    if (isStarted) {
                        start(pushConsumer);
                    }
                } catch (MQClientException e) {
                    log.error(e.getErrorMessage(), e);
                }
            }
        }
    }
}
