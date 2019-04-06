package com.xjbg.rocketmq.properties;

import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.impl.util.NameAddrUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * @author kesc
 * @since 2019/4/2
 */
@Getter
@Setter
public class OnsProducerProperties extends MqProducerProperties {
    private String instanceId;
    private OnsProperties onsProperties = new OnsProperties();

    public String getGroupId() {
        return "GID_" + super.getProducerGroup();
    }

    /**
     * @return
     * @see PropertyKeyConst
     */
    public Properties properties() {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.AccessKey, onsProperties.getAccessKey());
        properties.put(PropertyKeyConst.SecretKey, onsProperties.getSecretKey());
        properties.put(PropertyKeyConst.InstanceName, getInstanceName());
        properties.put(PropertyKeyConst.ONSAddr, onsProperties.getOnsAddr());
        properties.put(PropertyKeyConst.isVipChannelEnabled, isVipChannelEnabled());
        if (StringUtils.isNotBlank(getNamesrvAddr())) {
            properties.put(PropertyKeyConst.NAMESRV_ADDR, getNamesrvAddr());
            properties.put(PropertyKeyConst.INSTANCE_ID, NameAddrUtils.parseInstanceIdFromEndpoint(getNamesrvAddr()));
        }
        properties.put(PropertyKeyConst.GROUP_ID, getGroupId());
        if (StringUtils.isNotBlank(getInstanceId())) {
            properties.put(PropertyKeyConst.INSTANCE_ID, getInstanceId());
        }
        properties.put(PropertyKeyConst.SendMsgTimeoutMillis, getSendMsgTimeout());
        return properties;
    }
}
