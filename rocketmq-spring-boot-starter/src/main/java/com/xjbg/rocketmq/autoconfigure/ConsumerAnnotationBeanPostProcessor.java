/*
 * Created by luozy03 on 2018/1/15 15:49.
 */
package com.xjbg.rocketmq.autoconfigure;

import com.alibaba.fastjson.JSON;
import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.batch.BatchMessageListener;
import com.aliyun.openservices.ons.api.order.MessageOrderListener;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.xjbg.rocketmq.MqConstants;
import com.xjbg.rocketmq.container.AbstractConsumerContainer;
import com.xjbg.rocketmq.enums.ConsumerType;
import com.xjbg.rocketmq.enums.MessageListenerType;
import com.xjbg.rocketmq.filter.MessageRepeatFilter;
import com.xjbg.rocketmq.handler.Handler;
import com.xjbg.rocketmq.properties.MqConsumerProperties;
import com.xjbg.rocketmq.properties.OnsConsumerProperties;
import com.xjbg.rocketmq.util.ProfileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * process consumer annotation
 *
 * @author kesc
 * @version v1.0
 */
@Slf4j
public class ConsumerAnnotationBeanPostProcessor implements BeanPostProcessor, CommandLineRunner {

    @Resource
    private AbstractConsumerContainer consumerListenerContainer;
    @Resource
    private RocketMqProperties rocketMqProperties;
    @Resource
    private MessageRepeatFilter repeatFilter;


    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        Set<Consumer> emptySet = new HashSet<>();
        ReflectionUtils
                .doWithMethods(targetClass,
                        method ->
                                Optional.ofNullable(AnnotationUtils.findAnnotation(method, Consumer.class))
                                        .map(annos -> Stream.of(annos).collect(Collectors.toSet()))
                                        .orElse(emptySet)
                                        .forEach(rocketListener -> {
                                            if (rocketMqProperties.getConsumer().isEnable()) {
                                                Class<?>[] types = method.getParameterTypes();
                                                if (types.length != 1) {
                                                    throw new RuntimeException("mq handle param must only one");
                                                }
                                                boolean isMq = rocketMqProperties.getProduct().equals(MqConstants.PRODUCT_MQ);
                                                Handler handler = t -> method.invoke(bean, t);
                                                consumerListenerContainer.registerConsumer(isMq ? mqConsumerProperties(rocketListener) : onsConsumerProperties(rocketListener),
                                                        rocketListener.consumerType(),
                                                        isMq ? messageListener(rocketListener, handler, types[0], rocketListener.MessageListenerType()) : onsMessageListener(rocketListener, handler, types[0], rocketListener.MessageListenerType()));
                                            }
                                        }),
                        ReflectionUtils.USER_DECLARED_METHODS);
        return bean;
    }

    private <T> boolean innerOnsMessageListener(List<Message> msgs, Consumer consumer, Handler<T> handler, Class<T> paramType) {
        boolean success = true;
        for (Message message : msgs) {
            log.info("consume mq message topic:{},tag:{}, key:{}", ProfileUtil.getTopicPrefix() + consumer.topic(), message.getTag(), message.getKey());
            if (consumer.repeatCheck() && repeatFilter.isRepeat(message.getKey())) {
                continue;
            }
            try {
                handler.handle(JSON.parseObject(message.getBody(), paramType));
            } catch (Exception e) {
                log.error("rocketmq handle error,messages:{}", message, e);
                if (consumer.repeatCheck()) {
                    repeatFilter.reset(message.getKey());
                }
                success = false;
            }
        }
        return success;
    }

    private <T> Object onsMessageListener(Consumer consumer, Handler<T> handler, Class<T> paramType, MessageListenerType messageListenerType) {
        Object messageListener;
        boolean reConsume = consumer.maxReconsumeTimes() > 0;
        switch (messageListenerType) {
            case BATCH: {
                messageListener = (BatchMessageListener) (messages, context) -> {
                    boolean success = innerOnsMessageListener(messages, consumer, handler, paramType);
                    return success || !reConsume ? Action.CommitMessage : Action.ReconsumeLater;
                };
                break;
            }
            case ORDERLY: {
                messageListener = (MessageOrderListener) (message, context) -> {
                    boolean success = innerOnsMessageListener(Collections.singletonList(message), consumer, handler, paramType);
                    return success || !reConsume ? OrderAction.Success : OrderAction.Suspend;
                };
                break;
            }
            default: {
                messageListener = (com.aliyun.openservices.ons.api.MessageListener) (message, context) -> {
                    boolean success = innerOnsMessageListener(Collections.singletonList(message), consumer, handler, paramType);
                    return success || !reConsume ? Action.CommitMessage : Action.ReconsumeLater;
                };
            }
        }
        return messageListener;
    }

    private <T> boolean innerMessageListener(List<MessageExt> msgs, Consumer consumer, Handler<T> handler, Class<T> paramType) {
        boolean success = true;
        for (MessageExt messageExt : msgs) {
            log.info("consume mq message topic:{},tag:{}, key:{}", ProfileUtil.getTopicPrefix() + consumer.topic(), messageExt.getTags(), messageExt.getKeys());
            if (consumer.repeatCheck() && repeatFilter.isRepeat(messageExt.getKeys())) {
                continue;
            }
            try {
                handler.handle(JSON.parseObject(messageExt.getBody(), paramType));
            } catch (Exception e) {
                log.error("rocketmq handle error,messages:{}", messageExt, e);
                if (consumer.repeatCheck()) {
                    repeatFilter.reset(messageExt.getKeys());
                }
                success = false;
            }
        }
        return success;
    }

    private <T> Object messageListener(Consumer consumer, Handler<T> handler, Class<T> paramType, MessageListenerType messageListenerType) {
        MessageListener messageListener;
        boolean reConsume = consumer.maxReconsumeTimes() > 0;
        if (ConsumerType.PULL.equals(consumer.consumerType())) {
            return (PullTaskCallback) (mq, context) -> {
                MQPullConsumer mqPullConsumer = context.getPullConsumer();
                try {
                    //pull position in this message queue
                    long offset = mqPullConsumer.fetchConsumeOffset(mq, Boolean.FALSE);
                    if (offset < 0) {
                        offset = 0;
                    }
                    PullResult pullResult = mqPullConsumer.pull(mq, consumer.expression(), offset, consumer.maxPullNum());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            List<MessageExt> list = pullResult.getMsgFoundList();
                            for (MessageExt msg : list) {
                                if (consumer.repeatCheck() && repeatFilter.isRepeat(msg.getKeys())) {
                                    continue;
                                }
                                try {
                                    handler.handle(JSON.parseObject(msg.getBody(), paramType));
                                } catch (Exception e) {
                                    log.error("rocketmq handle error,messages:{}", msg, e);
                                    if (consumer.repeatCheck()) {
                                        repeatFilter.reset(msg.getKeys());
                                    }
                                }
                            }
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                    //update offset，client will sync to broker every 5s
                    mqPullConsumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
                    //more than 5s was suggest
                    context.setPullNextDelayTimeMillis(consumer.pullNextDelayTimeMillis());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            };
        }
        switch (messageListenerType) {
            case ORDERLY: {
                messageListener = (MessageListenerOrderly) (msgs, context) -> {
                    boolean success = innerMessageListener(msgs, consumer, handler, paramType);
                    return success || !reConsume ? ConsumeOrderlyStatus.SUCCESS : ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                };
                break;
            }
            default: {
                messageListener = (MessageListenerConcurrently) (msgs, context) -> {
                    boolean success = innerMessageListener(msgs, consumer, handler, paramType);
                    return success || !reConsume ? ConsumeConcurrentlyStatus.CONSUME_SUCCESS : ConsumeConcurrentlyStatus.RECONSUME_LATER;
                };
            }
        }
        return messageListener;
    }

    private <T extends MqConsumerProperties> void setProperties(Consumer consumer, T properties) {
        properties.setClientIP(rocketMqProperties.getConsumer().getClientIP());
        properties.setConsumeFromWhere(consumer.consumeFromWhere());
        properties.setConsumeMaxThread(consumer.consumeMaxThread());
        properties.setConsumeMessageModel(consumer.consumeMessageModel());
        properties.setConsumerGroup(ProfileUtil.getTopicPrefix() + consumer.consumerGroup());
        properties.setConsumerPullTimeoutMillis(consumer.consumerPullTimeoutMillis());
        properties.setConsumeTimestamp(consumer.consumeTimestamp());
        properties.setInstanceName(consumer.instanceName());
        properties.setConsumeTimeout(consumer.consumeTimeout());
        properties.setMaxReconsumeTimes(consumer.maxReconsumeTimes());
        properties.setUnitMode(consumer.unitMode());
        properties.setVipChannelEnabled(consumer.vipChannelEnabled());
        properties.setUnitName(consumer.unitName());
        properties.setNamesrvAddr(rocketMqProperties.getConsumer().getNamesrvAddr());
        properties.setTopic(consumer.withProfile() ? ProfileUtil.getTopicPrefix() + consumer.topic() : consumer.topic());
        properties.setExpression(consumer.expression());
    }

    private MqConsumerProperties mqConsumerProperties(Consumer consumer) {
        MqConsumerProperties consumerProperties = new MqConsumerProperties();
        setProperties(consumer, consumerProperties);
        return consumerProperties;
    }

    private OnsConsumerProperties onsConsumerProperties(Consumer consumer) {
        OnsConsumerProperties consumerProperties = new OnsConsumerProperties();
        setProperties(consumer, consumerProperties);
        if (StringUtils.isNotBlank(consumer.instanceId())) {
            consumerProperties.setInstanceId(consumer.instanceId());
        }
        consumerProperties.setOnsProperties(rocketMqProperties.getOns());
        return consumerProperties;
    }

    @Override
    public void run(String... args) throws Exception {
        consumerListenerContainer.startAll();
    }

}
