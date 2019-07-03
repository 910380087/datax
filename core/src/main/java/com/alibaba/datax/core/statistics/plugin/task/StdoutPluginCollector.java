package com.alibaba.datax.core.statistics.plugin.task;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.statistics.plugin.task.util.DirtyRecord;
import com.alibaba.fastjson.JSON;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jingxing on 14-9-9.
 */
public class StdoutPluginCollector extends AbstractTaskPluginCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(StdoutPluginCollector.class);

    private static final int DEFAULT_MAX_DIRTYNUM = 128;

    private AtomicInteger maxLogNum = new AtomicInteger(0);

    private AtomicInteger currentLogNum = new AtomicInteger(0);

    private String readerColumn = null;
    private String writerColumn = null;

    public StdoutPluginCollector(Configuration configuration, Communication communication,
                                 PluginType type) {
        super(configuration, communication, type);
        maxLogNum = new AtomicInteger(
                configuration.getInt(
                        CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_MAXDIRTYNUM,
                        DEFAULT_MAX_DIRTYNUM));
//        LOG.info("【Configuration】" + configuration.toString());
        //获取配置的列信息，可对比
        String readerColumnTemp = configuration.getString(
                        CoreConstant.DATAX_JOB_READER_COLUMN,
                        "");
        readerColumnTemp = readerColumnTemp.replace("index","序号");
        readerColumnTemp = readerColumnTemp.replace("type","类型");
        readerColumnTemp = readerColumnTemp.replace(" ","");
        readerColumnTemp = readerColumnTemp.replace("\n","");
        readerColumn = readerColumnTemp;
        String writerColumnTemp = configuration.getString(
                CoreConstant.DATAX_JOB_WRITER_COLUMN,
                "");
        writerColumnTemp = writerColumnTemp.replace("index","序号");
        writerColumnTemp = writerColumnTemp.replace("type","类型");
        writerColumnTemp = writerColumnTemp.replace(" ","");
        writerColumnTemp = writerColumnTemp.replace("\n","");
        writerColumn = writerColumnTemp;
    }

    private String formatDirty(final Record dirty, final Throwable t,
                               final String msg) {
        String dirtyDataInfo = "";
        dirtyDataInfo += "[报错信息]: " + msg + " \n";
        dirtyDataInfo += "[Reader列信息]: " + readerColumn + " \n";
        dirtyDataInfo += "[Writer列信息]: " + writerColumn + " \n";
        dirtyDataInfo += "[该脏数据记录]: " + DirtyRecord.asDirtyRecord(dirty)
                .getColumns() + " \n";
        dirtyDataInfo += "[脏数据异常]: " + t.toString();
        Map<String, Object> msgGroup = new HashMap<String, Object>();

        msgGroup.put("type", super.getPluginType().toString());
        if (StringUtils.isNotBlank(msg)) {
            msgGroup.put("message", msg);
        }
        if (null != t && StringUtils.isNotBlank(t.getMessage())) {
            msgGroup.put("exception", t.getMessage());
        }
        if (null != dirty) {
            msgGroup.put("record", DirtyRecord.asDirtyRecord(dirty)
                    .getColumns());
        }

//        return JSON.toJSONString(msgGroup);
        return dirtyDataInfo;
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t,
                                   String errorMessage) {
        int logNum = currentLogNum.getAndIncrement();
        if(logNum==0 && t!=null){
            LOG.error("", t);
        }
        if (maxLogNum.intValue() < 0 || currentLogNum.intValue() < maxLogNum.intValue()) {
            LOG.error("脏数据: \n"
                    + this.formatDirty(dirtyRecord, t, errorMessage));
        }

        super.collectDirtyRecord(dirtyRecord, t, errorMessage);
    }
}
