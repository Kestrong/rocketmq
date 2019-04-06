/*
 * Created by lzy on 2018/11/27 3:25 PM.
 */
package com.xjbg.rocketmq.manager;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.http.FormatType;
import com.aliyuncs.ons.model.v20190214.OnsConsumerStatusRequest;
import com.aliyuncs.ons.model.v20190214.OnsConsumerStatusResponse;
import com.aliyuncs.ons.model.v20190214.OnsRegionListRequest;
import com.aliyuncs.ons.model.v20190214.OnsRegionListResponse;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.xjbg.rocketmq.properties.OnsProperties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * ons region管理
 *
 * @author kesc
 * @version v1.0
 */
@Slf4j
public class OnsManagerApi {
    private static final byte[] LOCK = new byte[0];
    private volatile static String onsRegionId;

    public static IAcsClient acsClient(OnsProperties ons) {
        return new DefaultAcsClient(getProfile(ons));
    }

    public static IClientProfile getProfile(OnsProperties ons) {
        DefaultProfile.addEndpoint(ons.getRegionId(), "Ons", ons.getEndPoint());
        return DefaultProfile.getProfile(ons.getRegionId(), ons.getAccessKey(), ons.getSecretKey());
    }

    /**
     * 获取regionId
     *
     * @param ons the ons
     * @return the ons region id
     */
    public static String getONSRegionId(OnsProperties ons) {
        if (onsRegionId == null) {
            synchronized (LOCK) {
                if (onsRegionId == null) {
                    IAcsClient iAcsClient = acsClient(ons);
                    OnsRegionListRequest request = new OnsRegionListRequest();
                    request.setAcceptFormat(FormatType.JSON);
                    request.setPreventCache(System.currentTimeMillis());
                    try {
                        OnsRegionListResponse response = iAcsClient.getAcsResponse(request);
                        // 使用regionName来获取regionId,比如奇葩
                        onsRegionId = response.getData().stream().filter(regionDo -> regionDo.getRegionName().equals(ons.getRegionName()))
                                .findFirst().map(OnsRegionListResponse.RegionDo::getOnsRegionId).orElseThrow(() -> new RuntimeException("getONSRegionId error"));
                    } catch (Exception e) {
                        log.error("getONSRegionId error", e);
                    }
                }
            }
        }
        return onsRegionId;
    }

    /**
     * Check consumer.
     *
     * @param iAcsClient
     * @param groupId
     * @param instanceId
     */
    public static void checkConsumer(@NonNull IAcsClient iAcsClient, @NonNull String groupId, @NonNull String instanceId) {
        boolean checkFail = false;
        OnsConsumerStatusRequest request = new OnsConsumerStatusRequest();
        request.setPreventCache(System.currentTimeMillis());
        request.setAcceptFormat(FormatType.JSON);
        request.setGroupId(groupId);
        request.setInstanceId(instanceId);
        request.setDetail(Boolean.TRUE);
        try {
            OnsConsumerStatusResponse response = iAcsClient.getAcsResponse(request);
            OnsConsumerStatusResponse.Data data = response.getData();
            checkFail = data == null || !data.getOnline();
        } catch (Exception e) {
            log.error("checkConsumer error,GroupId:{},InstanceId:{},message:{}", groupId, instanceId, e);
        }
        if (checkFail) {
            throw new RuntimeException("consumer may not create on ons");
        }
    }
}
