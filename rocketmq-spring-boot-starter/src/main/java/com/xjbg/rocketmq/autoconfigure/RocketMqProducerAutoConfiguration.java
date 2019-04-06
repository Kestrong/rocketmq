package com.xjbg.rocketmq.autoconfigure;

import com.aliyun.openservices.ons.api.bean.OrderProducerBean;
import com.aliyun.openservices.ons.api.bean.ProducerBean;
import com.aliyun.openservices.ons.api.bean.TransactionProducerBean;
import com.xjbg.rocketmq.MqConstants;
import com.xjbg.rocketmq.factory.MqFactory;
import com.xjbg.rocketmq.listener.DefaultOnsTransactionListenerImpl;
import com.xjbg.rocketmq.properties.MqProducerProperties;
import com.xjbg.rocketmq.properties.OnsProducerProperties;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author kesc
 * @since 2019/4/3
 */
@Configuration
@EnableConfigurationProperties(RocketMqProperties.class)
@ConditionalOnProperty
        (
                prefix = RocketMqProperties.PREFIX,
                value = "producer.enable",
                havingValue = "true", matchIfMissing = true
        )
@AutoConfigureOrder(value = Integer.MAX_VALUE)
public class RocketMqProducerAutoConfiguration {

    @Autowired
    private RocketMqProperties rocketMqProperties;

    @Bean(name = "mqTransactionListener")
    @ConditionalOnMissingBean(name = "mqTransactionListener")
    @ConditionalOnExpression("'${" + MqConstants.PRODUCT_KEY + ":" + MqConstants.PRODUCT_MQ + "}'.equals('" + MqConstants.PRODUCT_MQ + "')"
            + "&&'${" + MqConstants.PRODUCER_MODE_KEY + ":" + MqConstants.NORMAL + "}'.equals('" + MqConstants.TRANSACTION + "')")
    public TransactionListener mqTransactionListener() {
        return new DefaultSpringMqTransactionListener();
    }

    private MqProducerProperties mqProducerProperties() {
        RocketMqProperties.Producer producer = rocketMqProperties.getProducer();
        assert producer != null;
        MqProducerProperties properties = new MqProducerProperties();
        BeanUtils.copyProperties(producer, properties);
        properties.setProducerGroup(rocketMqProperties.getProfile() + MqConstants.DASH + properties.getProducerGroup());
        return properties;
    }

    private OnsProducerProperties onsProducerProperties() {
        RocketMqProperties.Producer producer = rocketMqProperties.getProducer();
        assert producer != null;
        OnsProducerProperties properties = new OnsProducerProperties();
        BeanUtils.copyProperties(producer, properties);
        properties.setProducerGroup(rocketMqProperties.getProfile() + MqConstants.DASH + properties.getProducerGroup());
        properties.setOnsProperties(rocketMqProperties.getOns());
        return properties;
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnExpression("'${" + MqConstants.PRODUCT_KEY + ":" + MqConstants.PRODUCT_MQ + "}'.equals('" + MqConstants.PRODUCT_MQ + "')"
            + "&&'${" + MqConstants.PRODUCER_MODE_KEY + ":" + MqConstants.NORMAL + "}'.equals('" + MqConstants.NORMAL + "')")
    public DefaultMQProducer mqProducer() {
        return MqFactory.createProducer(mqProducerProperties());
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnExpression("'${" + MqConstants.PRODUCT_KEY + ":" + MqConstants.PRODUCT_MQ + "}'.equals('" + MqConstants.PRODUCT_MQ + "')"
            + "&&'${" + MqConstants.PRODUCER_MODE_KEY + ":" + MqConstants.NORMAL + "}'.equals('" + MqConstants.TRANSACTION + "')")
    public TransactionMQProducer transactionMQProducer(
            @Qualifier(value = "mqTransactionListener") TransactionListener mqTransactionListener
    ) {
        return MqFactory.createTransactionProducer(mqProducerProperties(), mqTransactionListener);
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnExpression("'${" + MqConstants.PRODUCT_KEY + ":" + MqConstants.PRODUCT_MQ + "}'.equals('" + MqConstants.PRODUCT_ONS + "')"
            + "&&'${" + MqConstants.PRODUCER_MODE_KEY + ":" + MqConstants.NORMAL + "}'.equals('" + MqConstants.NORMAL + "')")
    public ProducerBean onsProducer() {
        ProducerBean producerBean = new ProducerBean();
        producerBean.setProperties(onsProducerProperties().properties());
        return producerBean;
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnExpression("'${" + MqConstants.PRODUCT_KEY + ":" + MqConstants.PRODUCT_MQ + "}'.equals('" + MqConstants.PRODUCT_ONS + "')"
            + "&&'${" + MqConstants.PRODUCER_MODE_KEY + ":" + MqConstants.NORMAL + "}'.equals('" + MqConstants.ORDERLY + "')")
    public OrderProducerBean onsOrderProducer() {
        OrderProducerBean producerBean = new OrderProducerBean();
        producerBean.setProperties(onsProducerProperties().properties());
        return producerBean;
    }

    @Bean(name = "onsTransactionListener")
    @ConditionalOnMissingBean(name = "onsTransactionListener")
    @ConditionalOnExpression("'${" + MqConstants.PRODUCT_KEY + ":" + MqConstants.PRODUCT_MQ + "}'.equals('" + MqConstants.PRODUCT_ONS + "')"
            + "&&'${" + MqConstants.PRODUCER_MODE_KEY + ":" + MqConstants.NORMAL + "}'.equals('" + MqConstants.TRANSACTION + "')")
    public DefaultOnsTransactionListenerImpl onsTransactionListener() {
        return new DefaultSpringOnsTransactionListenerImpl();
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnExpression("'${" + MqConstants.PRODUCT_KEY + ":" + MqConstants.PRODUCT_MQ + "}'.equals('" + MqConstants.PRODUCT_ONS + "')"
            + "&&'${" + MqConstants.PRODUCER_MODE_KEY + ":" + MqConstants.NORMAL + "}'.equals('" + MqConstants.TRANSACTION + "')")
    public TransactionProducerBean onsTransactionProducer(
            @Qualifier(value = "onsTransactionListener") DefaultOnsTransactionListenerImpl onsTransactionListener
    ) {
        TransactionProducerBean producerBean = new TransactionProducerBean();
        producerBean.setProperties(onsProducerProperties().properties());
        producerBean.setLocalTransactionChecker(onsTransactionListener);
        return producerBean;
    }
}
