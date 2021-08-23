package org.apache.pinot.core.data.manager.realtime.zkConfigManager;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.core.data.manager.realtime.zkConfigManager.helper.ConfigureReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ZkWatcher {

    private static Logger logger = LoggerFactory.getLogger(ZkConfigureCenter.class);

    private static ZkWatcher instance = null;
    private static ConcurrentHashMap<String, Object> filter;
    private static ConcurrentHashMap<String, Object> defaultFilter=new ConcurrentHashMap<String, Object>();
    private static String fileName = "filter.json";
    public static ConcurrentHashMap<String, Object> getFilter() {
        if (!MapUtils.isNotEmpty(filter)) {
            return defaultFilter;
        }
        return filter;

    }
    static {

        defaultFilter.put("webex_aggregated_meeting_hdfs", Arrays.asList("SessUserLeave"));
        defaultFilter.put("webex_aggregated_tahoe_hdfs", Arrays.asList("Tel_Callout_End"));
        defaultFilter.put("logstash_cmse_servicediagnostic", Arrays.asList("FailOnJoinSession","SipAudioRecvInfo","SipVideoRecvInfo","ServerQos"));
        defaultFilter.put("logstash_telephony", Arrays.asList("FailOnJoinSession","SipAudioRecvInfo","SipVideoRecvInfo","ServerQos"));

        ConfigureReader reader = new ZkConfigureCenter();

        try {
            reader.watchCnfFile(fileName, new ConfigureReader.ChangeHandler() {
                @Override
                public void valueChange(JSONObject jsonObject) {
                    Map<String, Object> tmpFilter = new ConcurrentHashMap<>(); 
                    for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                        List<String> stringList= Arrays.asList((entry.getValue()).toString().split(","));
                        tmpFilter.put(entry.getKey(),stringList);
                    }
                    ZkWatcher.setFilter(tmpFilter);
                }
            });


        } catch (Exception e) {
            logger.error("listen data change error", e);
        }

    }


    public static void setFilter(Map<String, Object> filter) {
        
        ZkWatcher.filter = (ConcurrentHashMap<String, Object>) filter;
    }

    public static ZkWatcher getInstance() {

        if (instance == null) {
            synchronized (ZkWatcher.class) {
                if (instance == null) {
                    instance = new ZkWatcher();
                }
            }
        }
        return instance;
    }


}
