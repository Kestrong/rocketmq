package com.xjbg.rocketmq.message;

import com.alibaba.fastjson.JSON;
import com.xjbg.rocketmq.util.ProfileUtil;

import java.util.Optional;

/**
 * @author kesc
 * @since 2019/4/3
 */
public class MessageBuilder {
    private String topicPrefix = "";
    /**
     * 标识一类消息的逻辑名字,消息的逻辑管理单位.无论消息生产还是消费,都需要指定Topic
     */
    private String topic;
    /**
     * RocketMQ支持给在发送的时候给topic打tag,同一个topic的消息虽然逻辑管理是一样的.但是消费topic1的时候,如果你订阅的时候指定的是tagA,那么tagB的消息将不会投递
     */
    private String tags;
    /**
     * 自定义key,这里用来进行去重
     */
    private String keys;
    private Object body;
    /**
     * 延时,rocketmq不支持具体的延时时间,只支持使用延时等级
     * 需要在broker配置,rocketmq默认的配置如下
     * messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * 0表示不延时,1表示1s,2表示5s以此类推
     */
    private Integer delayTimeLevel;

    /**
     * 具体的延时时间,ons可以指定具体的延时时间,不过最多40天,单位:毫秒
     */
    private Long delayTime;

    public static MessageBuilder newProfileBuilder() {
        return new MessageBuilder().withProfile();
    }

    public static MessageBuilder newBuilder() {
        return new MessageBuilder();
    }

    public MessageBuilder withProfile() {
        this.topicPrefix = ProfileUtil.getTopicPrefix();
        return this;
    }

    /**
     * Topic message builder.
     *
     * @param topic the topic
     * @return the message builder
     */
    public MessageBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * Body message builder.
     *
     * @param body the body
     * @return the message builder
     */
    public MessageBuilder body(Object body) {
        this.body = body;
        return this;
    }

    /**
     * Tags message builder.
     *
     * @param tags the tags
     * @return the message builder
     */
    public MessageBuilder tags(String tags) {
        this.tags = tags;
        return this;
    }

    /**
     * Keys message builder.
     *
     * @param keys the keys
     * @return the message builder
     */
    public MessageBuilder keys(String keys) {
        this.keys = keys;
        return this;
    }

    /**
     * Delay time level message builder.
     *
     * @param delayTimeLevel the delay time level
     * @return the message builder
     */
    public MessageBuilder delay(int delayTimeLevel) {
        this.delayTimeLevel = delayTimeLevel;
        return this;
    }

    /**
     * Delay time level message builder.
     *
     * @param delayTime the delay time in mills
     * @return the message builder
     */
    public MessageBuilder delay(long delayTime) {
        this.delayTime = delayTime;
        return this;
    }

    /**
     * ons Message
     *
     * @return the ons message
     */
    public com.aliyun.openservices.ons.api.Message getOnsMessage() {
        com.aliyun.openservices.ons.api.Message message = new com.aliyun.openservices.ons.api.Message(topicPrefix + this.topic, this.tags, this.keys, JSON.toJSONBytes(this.body));
        Optional.ofNullable(this.delayTime).map(t -> System.currentTimeMillis() + t).ifPresent(message::setStartDeliverTime);
        ProfileUtil.remove();
        return message;
    }

    /**
     * rocket Message
     *
     * @return the rocket mq message
     */
    public org.apache.rocketmq.common.message.Message getRocketMQMessage() {
        org.apache.rocketmq.common.message.Message message = new org.apache.rocketmq.common.message.Message(topicPrefix + this.topic, this.tags, this.keys, JSON.toJSONBytes(this.body));
        Optional.ofNullable(this.delayTimeLevel).ifPresent(message::setDelayTimeLevel);
        ProfileUtil.remove();
        return message;
    }
}
