package com.xjbg.rocketmq.container;

import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.batch.BatchConsumer;
import com.aliyun.openservices.ons.api.batch.BatchMessageListener;
import com.aliyun.openservices.ons.api.order.MessageOrderListener;
import com.aliyun.openservices.ons.api.order.OrderConsumer;
import com.xjbg.rocketmq.enums.ConsumerType;
import com.xjbg.rocketmq.manager.OnsManagerApi;
import com.xjbg.rocketmq.properties.OnsConsumerProperties;
import com.xjbg.rocketmq.properties.OnsProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kesc
 * @since 2019/4/4
 */
@Slf4j
@Getter
public class OnsConsumerContainer extends AbstractConsumerContainer<OnsConsumerProperties> {
    private boolean checkCreated = true;
    private OnsProperties onsProperties;
    /**
     * groupId:consumerInfo
     */
    private final ConcurrentHashMap<String, ConsumerInfo> onsConsumersMap = new ConcurrentHashMap<>();

    private void checkConsumerCreated(final ConsumerInfo consumerInfo) {
        OnsManagerApi.checkConsumer(OnsManagerApi.acsClient(onsProperties), consumerInfo.getGroupId(), consumerInfo.getInstanceId());
    }

    private void start(ConsumerInfo onsConsumer) {
        onsConsumer.getAdmin().start();
        if (isCheckCreated()) {
            checkConsumerCreated(onsConsumer);
        }
    }

    @Override
    void doStartAll() {
        for (Map.Entry<String, ConsumerInfo> entry : onsConsumersMap.entrySet()) {
            start(entry.getValue());
            log.info("start ons consumer:{}", entry.getKey());
        }
    }

    @Override
    void doShutdownAll() {
        for (Map.Entry<String, ConsumerInfo> entry : onsConsumersMap.entrySet()) {
            entry.getValue().getAdmin().shutdown();
            log.info("shutdown ons consumer:{}", entry.getKey());
        }
    }

    @Override
    void doRegisterConsumer(final OnsConsumerProperties properties, final ConsumerType consumerType, final Object messageListener) {
        if (onsConsumersMap.containsKey(properties.getGroupId())) {
            return;
        }
        switch (consumerType) {
            case BATCH: {
                BatchConsumer batchConsumer = ONSFactory.createBatchConsumer(properties.properties());
                batchConsumer.subscribe(properties.getTopic(), properties.getExpression(), (BatchMessageListener) messageListener);
                ConsumerInfo consumerInfo = new ConsumerInfo(properties.getGroupId(), properties.getInstanceId(), batchConsumer, consumerType);
                onsConsumersMap.put(properties.getGroupId(), consumerInfo);
                if (isStarted) {
                    start(consumerInfo);
                }
                break;
            }
            case ORDERLY: {
                OrderConsumer orderConsumer = ONSFactory.createOrderedConsumer(properties.properties());
                orderConsumer.subscribe(properties.getTopic(), properties.getExpression(), (MessageOrderListener) messageListener);
                ConsumerInfo consumerInfo = new ConsumerInfo(properties.getGroupId(), properties.getInstanceId(), orderConsumer, consumerType);
                onsConsumersMap.put(properties.getGroupId(), consumerInfo);
                if (isStarted) {
                    start(consumerInfo);
                }
                break;
            }
            default: {
                Consumer consumer = ONSFactory.createConsumer(properties.properties());
                consumer.subscribe(properties.getTopic(), properties.getExpression(), (MessageListener) messageListener);
                ConsumerInfo consumerInfo = new ConsumerInfo(properties.getGroupId(), properties.getInstanceId(), consumer, consumerType);
                onsConsumersMap.put(properties.getGroupId(), consumerInfo);
                if (isStarted) {
                    start(consumerInfo);
                }
            }
        }
    }

    public void setCheckCreated(boolean checkCreated) {
        this.checkCreated = checkCreated;
    }

    public void setOnsProperties(OnsProperties onsProperties) {
        this.onsProperties = onsProperties;
    }

    @Data
    @AllArgsConstructor
    class ConsumerInfo {
        private String groupId;
        private String instanceId;
        private Admin admin;
        /**
         * BATCH ORDERLY DEFAULT
         */
        private ConsumerType consumerType;
    }
}
