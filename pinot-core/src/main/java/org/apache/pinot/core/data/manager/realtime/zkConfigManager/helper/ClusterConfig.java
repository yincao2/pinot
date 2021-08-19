package org.apache.pinot.core.data.manager.realtime.zkConfigManager.helper;


public class ClusterConfig {
    private static String zkUrl;
    private static ClusterConfig instance = null;
    public static ClusterConfig getInstance() {
        if (instance == null) {
            synchronized (ClusterConfig.class) {
                if (instance == null) {
                    instance = new ClusterConfig();
                }
            }
        }
        return instance;
    }
    public static String getZkUrl() {
        return zkUrl;
    }
    public static void setZkUrl(String zkUrl) {
        ClusterConfig.zkUrl = zkUrl;
    }
}
