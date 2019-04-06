package com.xjbg.rocketmq.properties;

import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.impl.util.NameAddrUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * @author kesc
 * @since 2019/4/3
 */
@Getter
@Setter
public class OnsConsumerProperties extends MqConsumerProperties {
    private String instanceId;
    private OnsProperties onsProperties = new OnsProperties();

    public String getGroupId() {
        return "CID_" + super.getConsumerGroup();
    }

    public Properties properties() {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.GROUP_ID, getConsumerGroup());
        properties.put(PropertyKeyConst.AccessKey, onsProperties.getAccessKey());
        properties.put(PropertyKeyConst.SecretKey, onsProperties.getSecretKey());
        properties.put(PropertyKeyConst.ONSAddr, onsProperties.getOnsAddr());
        if (getMaxReconsumeTimes() > 0) {
            properties.put(PropertyKeyConst.MaxReconsumeTimes, getMaxReconsumeTimes());
        }
        if (getConsumeTimeout() > 0) {
            properties.put(PropertyKeyConst.ConsumeTimeout, getConsumeTimeout());
        }
        if (getConsumeMaxThread() > 0) {
            properties.put(PropertyKeyConst.ConsumeThreadNums, getConsumeMaxThread());
        }
        if (StringUtils.isNotBlank(getNamesrvAddr())) {
            properties.put(PropertyKeyConst.NAMESRV_ADDR, getNamesrvAddr());
            properties.put(PropertyKeyConst.INSTANCE_ID, NameAddrUtils.parseInstanceIdFromEndpoint(getNamesrvAddr()));
        }
        if (StringUtils.isNotBlank(getInstanceId())) {
            properties.put(PropertyKeyConst.INSTANCE_ID, getInstanceId());
        }
        properties.put(PropertyKeyConst.MessageModel, getConsumeMessageModel().getModeCN());
        properties.put(PropertyKeyConst.isVipChannelEnabled, isVipChannelEnabled());
        return properties;
    }
}
