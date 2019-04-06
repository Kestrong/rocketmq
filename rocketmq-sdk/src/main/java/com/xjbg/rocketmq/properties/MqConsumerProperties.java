package com.xjbg.rocketmq.properties;

import com.xjbg.rocketmq.MqConstants;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * @author kesc
 * @since 2019/4/2
 */
@Getter
@Setter
public class MqConsumerProperties {
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    private String clientIP = RemotingUtil.getLocalAddress();
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    /**
     * 标识一类Consumer的集合名称,这类Consumer通常消费一类消息,且消费逻辑一致
     */
    private String consumerGroup = MqConstants.DEFAULT_CONSUMER_GROUP;

    /**
     * 消费超时时间 minutes
     */
    private int consumeTimeout = -1;

    /**
     * 最大重试次数
     */
    private int maxReconsumeTimes = 16;

    /**
     * 最大线程数
     */
    private int consumeMaxThread = -1;

    /**
     * 新的消费者从哪里开始消费
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * 消费者第一次消费的时间点
     */
    private String consumeTimestamp = "";

    /**
     * 消费模式,默认为集群模式
     */
    private MessageModel consumeMessageModel = MessageModel.CLUSTERING;

    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;

    private String unitName;

    /**
     * The socket timeout in milliseconds
     */
    private long consumerPullTimeoutMillis = 1000 * 10;
    private boolean vipChannelEnabled = false;
    /**
     * 要消费的topic
     *
     * @return the string
     */
    private String topic;

    /**
     * 过滤的表达式,*代表订阅所有tag,多个tag使用||连接
     *
     * @return the string
     */
    private String expression = "*";
}
