package com.xjbg.rocketmq.autoconfigure;

import com.xjbg.rocketmq.MqConstants;
import com.xjbg.rocketmq.properties.OnsProperties;
import com.xjbg.rocketmq.util.ProfileUtil;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author kesc
 * @since 2019/4/3
 */
@Getter
@Setter
@ConfigurationProperties(prefix = RocketMqProperties.PREFIX)
public class RocketMqProperties implements InitializingBean {
    public static final String PREFIX = "rocketmq";
    private String profile;
    /**
     * spring profile
     */
    @Value("${spring.profiles.active:}")
    private String springProfiles;
    private String product = MqConstants.PRODUCT_MQ;
    private Producer producer = new Producer();
    private Consumer consumer = new Consumer();
    private OnsProperties ons;

    public String getProfile() {
        if (StringUtils.isNotBlank(profile)) {
            return profile;
        } else if (StringUtils.isNotBlank(springProfiles)) {
            return springProfiles;
        }
        return MqConstants.EMPTY;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        ProfileUtil.setTopicProfile(getProfile());
    }

    @Data
    public static class Producer {
        private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
        private String producerGroup = MqConstants.DEFAULT_PRODUCER_GROUP;
        private int sendMsgTimeout = 3000;
        private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
        private boolean vipChannelEnabled = false;
        private int defaultTopicQueueNums = 4;
        private int compressMsgBodyOverHowmuch = 1024 * 4;
        private int maxMessageSize = 1024 * 1024 * 4;
        private String clientIp = RemotingUtil.getLocalAddress();
        private int heartbeatBrokerInterval = 1000 * 30;
        private boolean useTLS = TlsSystemConfig.tlsEnable;
        private boolean unitMode = false;
        private String unitName;
        private String instanceId;
        private String mode = MqConstants.NORMAL;
        private boolean enable = true;
    }

    @Data
    public static class Consumer {
        private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
        private String clientIP = RemotingUtil.getLocalAddress();
        private boolean enable = true;
        private boolean checkCreated = true;
    }
}
