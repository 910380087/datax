package com.alibaba.datax.plugin.reader.gbase;

import ch.qos.logback.classic.Logger;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.reader.gbase.error.ErrorEnum;
import com.alibaba.datax.plugin.reader.gbase.error.GbaseError;
import com.alibaba.datax.plugin.reader.gbase.util.ConnectionUtil;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

public class GbaseReader extends Reader {

//    private static final DataBaseType DATABASE_TYPE = DataBaseType.GreenPlumSQL;

    private static Logger logger = (Logger) LoggerFactory.getLogger("GbaseReader");

    public static class Job extends Reader.Job {

        private Configuration originalConfig;
        private CommonRdbmsReader.Job commonRdbmsReaderMaster;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            logger.info("GbaseReader");
            logger.info("GbaseReader:");
            logger.info(originalConfig.toString());


            GbaseError gbaseError = new GbaseError(String.valueOf(ErrorEnum.GET_CONFIG_ERROR.getCode()),
                    ErrorEnum.GET_CONFIG_ERROR.getMsg());

            Connection connection = ConnectionUtil.getConnection(
                    this.originalConfig.getNecessaryValue("jdbcUrl", gbaseError),
                    this.originalConfig.getNecessaryValue("username", gbaseError),
                    this.originalConfig.getNecessaryValue("password", gbaseError)
            );



//            this.originalConfig.set(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE, Constant.DEFAULT_FETCH_SIZE);
//            int fetchSize = this.originalConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
//                    Constant.DEFAULT_FETCH_SIZE);
//            if (fetchSize < 1) {
//                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
//                        String.format("您配置的fetchSize有误，根据DataX的设计，fetchSize : [%d] 设置值不能小于 1.", fetchSize));
//            }
//            this.originalConfig.set(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE, fetchSize);

//            this.commonRdbmsReaderMaster = new CommonRdbmsReader.Job(DATABASE_TYPE);
//            this.commonRdbmsReaderMaster.init(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderMaster.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderMaster.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderMaster.destroy(this.originalConfig);
        }

    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderSlave;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
//            this.commonRdbmsReaderSlave = new CommonRdbmsReader.Task(DATABASE_TYPE,super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderSlave.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);

            this.commonRdbmsReaderSlave.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderSlave.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderSlave.destroy(this.readerSliceConfig);
        }

    }

}
