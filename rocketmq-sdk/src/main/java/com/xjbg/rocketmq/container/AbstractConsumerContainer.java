package com.xjbg.rocketmq.container;

import com.xjbg.rocketmq.enums.ConsumerType;
import com.xjbg.rocketmq.properties.MqConsumerProperties;

/**
 * @author kesc
 * @since 2019/4/4
 */
public abstract class AbstractConsumerContainer<T extends MqConsumerProperties> {
    protected volatile transient boolean isStarted = false;
    protected static final byte[] LOCK = new byte[0];


    public void startAll() {
        synchronized (LOCK) {
            doStartAll();
            isStarted = true;
        }
    }

    public void shutdownAll() {
        synchronized (LOCK) {
            doShutdownAll();
            isStarted = false;
        }
    }

    public void registerConsumer(final T properties, final ConsumerType consumerType, final Object messageListener) {
        synchronized (LOCK) {
            doRegisterConsumer(properties, consumerType, messageListener);
        }
    }

    abstract void doStartAll();

    abstract void doShutdownAll();

    abstract void doRegisterConsumer(final T properties, final ConsumerType consumerType, final Object messageListener);

}
