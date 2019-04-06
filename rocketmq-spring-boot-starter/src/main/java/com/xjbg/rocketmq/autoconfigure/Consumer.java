package com.xjbg.rocketmq.autoconfigure;

import com.xjbg.rocketmq.MqConstants;
import com.xjbg.rocketmq.enums.ConsumerType;
import com.xjbg.rocketmq.enums.MessageListenerType;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.lang.annotation.*;

/**
 * the method annotate Consumer must have only one parameter which is the type you put into message body
 *
 * @author kesc
 * @since 2019/4/4
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Consumer {
    /**
     * 要消费的topic
     *
     * @return the string
     */
    String topic();

    /**
     * 过滤的表达式,*代表订阅所有tag,多个tag使用||连接
     *
     * @return the string
     */
    String expression() default "*";

    String instanceName() default "DEFAULT";

    /**
     * 标识一类Consumer的集合名称,这类Consumer通常消费一类消息,且消费逻辑一致
     */
    String consumerGroup() default MqConstants.DEFAULT_CONSUMER_GROUP;

    String instanceId() default "";

    /**
     * 消费超时时间 minutes
     */
    int consumeTimeout() default -1;

    /**
     * 最大重试次数
     */
    int maxReconsumeTimes() default 16;

    /**
     * 最大线程数
     */
    int consumeMaxThread() default -1;

    /**
     * 最大拉取数量 仅在ConsumerType=PULL时有效
     *
     * @return
     */
    int maxPullNum() default 16;

    /**
     * 拉取间隔 建议大于5秒避免重复拉取
     *
     * @return
     */
    int pullNextDelayTimeMillis() default 10 * 1000;

    /**
     * 新的消费者从哪里开始消费
     */
    ConsumeFromWhere consumeFromWhere() default ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * 消费者第一次消费的时间点
     */
    String consumeTimestamp() default "";

    /**
     * 消费模式,默认为集群模式
     */
    MessageModel consumeMessageModel() default MessageModel.CLUSTERING;

    /**
     * Whether the unit of subscription group
     */
    boolean unitMode() default false;

    String unitName() default "";

    /**
     * The socket timeout in milliseconds
     */
    long consumerPullTimeoutMillis() default 1000 * 10;

    boolean vipChannelEnabled() default false;

    boolean repeatCheck() default false;

    /**
     * please acknowledged that pull consumer is not re-consume here
     * we suggest to use push consumer(default)
     *
     * @return
     */
    ConsumerType consumerType() default ConsumerType.DEFAULT;

    MessageListenerType MessageListenerType() default MessageListenerType.DEFAULT;

    boolean withProfile() default true;
}
