package com.xjbg.rocketmq.properties;

import com.xjbg.rocketmq.MqConstants;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

/**
 * @author kesc
 * @since 2019/4/2
 */
@Getter
@Setter
public class MqProducerProperties {
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
}
