package org.apache.pinot.core.data.manager.realtime.zkConfigManager.helper;

import com.alibaba.fastjson.JSONObject;

/**
 *
 * ConfigureReader
 *
 */
public interface ConfigureReader {
    /**
     * load config file
     * @param fileName fileName
     * @return properties
     */
    JSONObject loadCnfFile(String fileName);
    /**
     * watch config change
     * @param fileName fileName
     * @param changeHandler handler
     */
    void watchCnfFile(String fileName, ChangeHandler changeHandler);

    /**
     *
     * ChangeHandler
     */
    interface ChangeHandler {
        /**
         *
         * @param jsonObject properties
         */
        void valueChange(JSONObject jsonObject);
    }
}
