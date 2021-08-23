package org.apache.pinot.core.data.manager.realtime.zkConfigManager;

import com.alibaba.fastjson.JSONObject;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.pinot.core.data.manager.realtime.zkConfigManager.helper.ClusterConfig;
import org.apache.pinot.core.data.manager.realtime.zkConfigManager.helper.ConfigureReader;
import org.apache.pinot.core.data.manager.realtime.zkConfigManager.helper.MyZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkConfigureCenter implements ConfigureReader {
    private static Logger logger = LoggerFactory.getLogger(ZkConfigureCenter.class);
    private String confFilePath;
    private static final String default_confRootPath = "/mats/pinot";
    private static final String default_fileName = "filter.json";
    private String zkUrl;
    private ZkClient client;

    public ZkConfigureCenter() {
        this(default_confRootPath, default_fileName);
    }
    public ZkConfigureCenter(String path, String fileName) {
        if (path == null || path.trim().equals("")) {
            logger.error("path is null");
        }
        confFilePath = path + "/config";
        String fileNode = path + "/config" + "/" + fileName + "";
        zkUrl=  ClusterConfig.getZkUrl();
        client = new ZkClient(zkUrl);
        client.setZkSerializer(new MyZkSerializer());
        if (!this.client.exists(fileNode)) {
            try {
                this.client.createPersistent(fileNode, true);
            } catch (ZkNodeExistsException e) {
                logger.error("path is not exist", e);
            }
        }
    }


    @Override
    public JSONObject loadCnfFile(String fileName) {
        if (!fileName.startsWith("/")) {
            fileName = confFilePath + "/" + fileName;
        }
        return JSONObject.parseObject(client.readData(fileName, true));
    }


    @Override
    public void watchCnfFile(String fileName, ChangeHandler changeHandler) {

        if (!fileName.startsWith("/")) {
            fileName = confFilePath + "/" + fileName;
        }
        String finalFileName = fileName;
        client.subscribeDataChanges(fileName, new IZkDataListener() {
            @Override
            public void handleDataDeleted(String dataPath)  {
            }

            @Override
            public void handleDataChange(String dataPath, Object data)  {
                logger.info("Trigger data change" + dataPath);
                JSONObject jsonObject = loadCnfFile(finalFileName);
                changeHandler.valueChange(jsonObject);
            }
        });
    }


}
