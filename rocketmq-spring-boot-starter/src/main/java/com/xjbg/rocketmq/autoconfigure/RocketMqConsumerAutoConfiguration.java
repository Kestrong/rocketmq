package com.xjbg.rocketmq.autoconfigure;

import com.xjbg.rocketmq.MqConstants;
import com.xjbg.rocketmq.container.MqConsumerContainer;
import com.xjbg.rocketmq.container.OnsConsumerContainer;
import com.xjbg.rocketmq.filter.DefaultMessageRepeatFilterImpl;
import com.xjbg.rocketmq.filter.MessageRepeatFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author kesc
 * @since 2019/4/4
 */
@Configuration
@EnableConfigurationProperties(RocketMqProperties.class)
@ConditionalOnProperty
        (
                prefix = RocketMqProperties.PREFIX,
                name = "consumer.enable",
                havingValue = "true", matchIfMissing = true
        )
@AutoConfigureOrder(value = Integer.MAX_VALUE)
public class RocketMqConsumerAutoConfiguration {

    @Autowired
    private RocketMqProperties rocketMqProperties;

    @Bean(name = "messageRepeatFilter")
    @ConditionalOnMissingBean(name = "messageRepeatFilter")
    public MessageRepeatFilter messageRepeatFilter() {
        return new DefaultMessageRepeatFilterImpl();
    }

    @Bean(name = "consumerContainer", destroyMethod = "shutdownAll")
    @ConditionalOnProperty(value = MqConstants.PRODUCT_KEY, havingValue = MqConstants.PRODUCT_MQ, matchIfMissing = true)
    public MqConsumerContainer mqConsumerContainer() {
        return new MqConsumerContainer();
    }

    @Bean(name = "consumerContainer", destroyMethod = "shutdownAll")
    @ConditionalOnProperty(value = MqConstants.PRODUCT_KEY, havingValue = MqConstants.PRODUCT_ONS)
    public OnsConsumerContainer onsConsumerContainer() {
        OnsConsumerContainer onsConsumerContainer = new OnsConsumerContainer();
        onsConsumerContainer.setCheckCreated(rocketMqProperties.getConsumer().isCheckCreated());
        onsConsumerContainer.setOnsProperties(rocketMqProperties.getOns());
        return onsConsumerContainer;
    }

    @Bean
    @ConditionalOnBean(name = "consumerContainer")
    public ConsumerAnnotationBeanPostProcessor consumerAnnotationBeanPostProcessor() {
        return new ConsumerAnnotationBeanPostProcessor();
    }
}
