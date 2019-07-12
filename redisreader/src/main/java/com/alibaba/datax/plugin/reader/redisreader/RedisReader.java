package com.alibaba.datax.plugin.reader.redisreader;

import ch.qos.logback.classic.Logger;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.reader.redisreader.util.DefaultRedisPoolConfig;
import com.alibaba.datax.plugin.reader.redisreader.util.JedisUtil;
import com.alibaba.datax.plugin.reader.redisreader.util.RedisOperUtil;
import com.alibaba.datax.plugin.reader.redisreader.util.RedisUtil;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class RedisReader extends Reader {


    private static Logger logger = (Logger) LoggerFactory.getLogger("RedisReader");
    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static Properties props = new Properties();

    private static String mode = null;
    private static String servers = null;
    private static String password = null;


    private static Lock lock = new ReentrantLock();
    private static long totalDataNumber = 0;


    public static long getTotalDataNumber() {
        return totalDataNumber;
    }

    public static void setTotalDataNumber(long totalDataNumber) {
        RedisReader.totalDataNumber = totalDataNumber;
    }

    public static void increaseTotalDataNumber(){
        lock.lock();
        totalDataNumber++;
        lock.unlock();
    }

    public static class Job extends Reader.Job {
        private Configuration originalConfig = null;


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            logger.info("RedisReader");

            //connection init
            mode = this.originalConfig.getNecessaryValue(Constant.MODE,
                    DBUtilErrorCode.CONF_ERROR);
            if (!Strings.isNullOrEmpty(mode) && Constant.MODE_SINGLE.equals(mode)) {
                logger.info("RedisReader redis 单例模式 ");
                servers = this.originalConfig.getNecessaryValue(Constant.BOOTSTRAP_SERVERS,
                        DBUtilErrorCode.CONF_ERROR);
                if (Strings.isNullOrEmpty(servers)) {
                    logger.error("RedisReader server host and name must not be null");
                    throw DataXException.asDataXException(RedisReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            new RuntimeException());
                }

                password = this.originalConfig.getString(Constant.PASSWORD,
                        null);

            } else if (!Strings.isNullOrEmpty(mode) && Constant.MODE_CLUSTER.equals(mode)) {
                logger.info("RedisReader redis 集群模式 ");
                servers = this.originalConfig.getNecessaryValue(Constant.BOOTSTRAP_SERVERS,
                        DBUtilErrorCode.CONF_ERROR);
                if (Strings.isNullOrEmpty(servers)) {
                    logger.error("RedisReader server host and name must not be null");
                    throw DataXException.asDataXException(RedisReaderErrorCode.CONFIG_INVALID_CLUSTER_EXCEPTION,
                            new RuntimeException());
                }

                password = this.originalConfig.getString(Constant.PASSWORD,
                        null);
            }else if (!Strings.isNullOrEmpty(mode) && Constant.MODE_SENTINEL.equals(mode)) {
                logger.info("RedisReader redis 哨兵模式 ");
                servers = this.originalConfig.getNecessaryValue(Constant.BOOTSTRAP_SERVERS,
                        DBUtilErrorCode.CONF_ERROR);
                if (Strings.isNullOrEmpty(servers)) {
                    logger.error("RedisReader server host and name must not be null");
                    throw DataXException.asDataXException(RedisReaderErrorCode.CONFIG_INVALID_CLUSTER_EXCEPTION,
                            new RuntimeException());
                }

                password = this.originalConfig.getString(Constant.PASSWORD,
                        null);
            }


            logger.info("RedisReader init success");
        }

        @Override
        public void prepare() {
            logger.info("prepare() begin...");
            super.prepare();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            logger.info("split() begin...");
            List<Configuration> list = new ArrayList<Configuration>();
            list.add(this.originalConfig);
            logger.info("split() end...");
            return list;
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

    }

    public static class Task extends Reader.Task {
        private Configuration readerSliceConfig;

        private String hashKey = null;
        private String listKey = null;
        private String setKey = null;

//        private RedisUtil redisUtil;

        private JedisUtil jedisUtil = null;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            logger.info("RedisReader task init");

            mode = this.readerSliceConfig.getNecessaryValue(Constant.MODE,
                    DBUtilErrorCode.CONF_ERROR);
            if (!Strings.isNullOrEmpty(mode) && Constant.MODE_SINGLE.equals(mode)) {
                logger.info("RedisReader redis 单例模式 ");
                logger.info("RedisReader redis 主机信息: " + servers);


                String[] args = servers.split(":");
                jedisUtil = JedisUtil.getJedisUtil(args[0],Integer.valueOf(args[1]),password);

            } else if (!Strings.isNullOrEmpty(mode) && Constant.MODE_CLUSTER.equals(mode)) {
                logger.info("RedisReader redis 集群模式 ");
                logger.info("RedisReader redis 主机信息: " + servers);
            }else if (!Strings.isNullOrEmpty(mode) && Constant.MODE_SENTINEL.equals(mode)) {
                logger.info("RedisReader redis 哨兵模式 ");
                logger.info("RedisReader redis 主机信息: " + servers);
            }


            // check protection
            String hashKey = this.readerSliceConfig.getString(Constant.HASH_KEY,null);
//            logger.info("[RedisWriter] get hash key: " + hashKey);
            if (!Strings.isNullOrEmpty(hashKey)) {
                this.hashKey = hashKey;
            }

            String listKey = this.readerSliceConfig.getString(Constant.LIST_KEY,null);
            if (!Strings.isNullOrEmpty(listKey)) {
                this.listKey = listKey;
            }


            String setKey = this.readerSliceConfig.getString(Constant.SET_KEY,null);
            if (!Strings.isNullOrEmpty(setKey)) {
                this.setKey = setKey;
            }




        }

        @Override
        public void prepare() {
            logger.info("RedisReader task prepare");
        }

        @Override
        public void post() {
            logger.info("RedisReader task post");
        }

        @Override
        public void destroy() {
            logger.info("RedisReader task destroy");
            jedisUtil.destroy();
        }


        @Override
        public void startRead(RecordSender recordSender) {
            logger.info("【RedisReader】 startRead ");
            if (hashKey != null){
                if (jedisUtil != null){
                    List<String> list = jedisUtil.hvals(hashKey);
                    Iterator<String> iterator = list.iterator();
                    while (iterator.hasNext()){
//                      System.out.println("【line的list的next遍历】" + line );
                        transportOneLineToRecord(iterator.next(),recordSender,
                                this.getTaskPluginCollector(),this.readerSliceConfig);
                    }
                }

            }else if (listKey != null){

            }else if (setKey != null){

            }else {
                return;
            }


        }




        //解析数据格式

        /**
         * [{
         * "type":"double",
         * "value":1,
         * "index":0
         * },{
         * "type":"double",
         * "value":1,
         * "index":0
         * }]
         *
         * @return
         */
        public void transportOneLineToRecord(String tranLine,
                                             RecordSender recordSender,
                                             TaskPluginCollector taskPluginCollector,
                                             Configuration readerSliceConfig) {
            List<ColumnEntry> column = getListColumnEntry(readerSliceConfig, Key.COLUMN);
            String delimiterInStr = readerSliceConfig.getString(Key.FIELD_DELIMITER);
            String nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT);
            String[] parseRows;
            String line = tranLine;
            if (line != null)  //读取到的内容给line变量
            {
                parseRows = line.split(delimiterInStr, -1);
                transportOneRecord(recordSender,
                        column, parseRows, nullFormat, taskPluginCollector);
            }
        }



        public static List<ColumnEntry> getListColumnEntry(
                Configuration configuration, final String path) {
            List<JSONObject> lists = configuration.getList(path, JSONObject.class);
            if (lists == null) {
                return null;
            }
            List<ColumnEntry> result = new ArrayList<ColumnEntry>();
            for (final JSONObject object : lists) {
                result.add(JSON.parseObject(object.toJSONString(),
                        ColumnEntry.class));
            }
            return result;
        }


        public static Record transportOneRecord(RecordSender recordSender,
                                                List<ColumnEntry> columnConfigs, String[] sourceLine,
                                                String nullFormat, TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();
            Column columnGenerated = null;

            // 创建都为String类型column的record
            if (null == columnConfigs || columnConfigs.size() == 0) {
                for (String columnValue : sourceLine) {
                    // not equalsIgnoreCase, it's all ok if nullFormat is null
                    if (columnValue.equals(nullFormat)) {
                        columnGenerated = new StringColumn(null);
                    } else {
                        columnGenerated = new StringColumn(columnValue);
                    }
                    record.addColumn(columnGenerated);
                }
                recordSender.sendToWriter(record);
            } else {
                try {
                    for (ColumnEntry columnConfig : columnConfigs) {
                        String columnType = columnConfig.getType();
                        Integer columnIndex = columnConfig.getIndex();
                        String columnConst = columnConfig.getValue();

                        String columnValue = null;

                        if (null == columnIndex && null == columnConst) {
                            throw DataXException
                                    .asDataXException(
                                            UnstructuredStorageReaderErrorCode.NO_INDEX_VALUE,
                                            "由于您配置了type, 则至少需要配置 index 或 value");
                        }

                        if (null != columnIndex && null != columnConst) {
                            throw DataXException
                                    .asDataXException(
                                            UnstructuredStorageReaderErrorCode.MIXED_INDEX_VALUE,
                                            "您混合配置了index, value, 每一列同时仅能选择其中一种");
                        }

                        if (null != columnIndex) {
                            if (columnIndex >= sourceLine.length) {

                                String message = String
                                        .format("源文件问题，您尝试读取的列越界,源文件该行有 [%s] 列,您尝试读取第 [%s] 列, 数据详情[%s] \r\n ",
                                                sourceLine.length, columnIndex + 1,
                                                StringUtils.join(sourceLine, ","));
                                System.out.println(message);
                                System.out.println("数组越界，无法格式化，下一条");
                                continue;
//                                throw new IndexOutOfBoundsException(message);
                            }

                            columnValue = sourceLine[columnIndex];
                        } else {
                            columnValue = columnConst;
                        }
                        Type type = Type.valueOf(columnType.toUpperCase());
                        // it's all ok if nullFormat is null
                        if (columnValue.equals(nullFormat)) {
                            columnValue = null;
                        }
                        switch (type) {

                            case LONG:
                                try {
                                    if ("".equals(columnValue) || columnValue == null
                                            || " ".contentEquals(columnValue) || "  ".contentEquals(columnValue))
                                    {
                                        String data = null;
                                        columnGenerated = new LongColumn(data);
                                    }else {
                                        columnGenerated = new LongColumn(columnValue);
                                    }
                                } catch (Exception e) {
                                    throw new IllegalArgumentException(String.format(
                                            "类型转换错误, 无法将数据[%s],类型[%s],序号[%s] 转换为[%s]", columnValue,columnType, columnIndex,
                                            "LONG"));
                                }
                                break;
                            case DOUBLE:
                                try {
                                    if ("".equals(columnValue) || columnValue == null
                                            || " ".contentEquals(columnValue) || "  ".contentEquals(columnValue))
                                    {
                                        String data = null;
                                        columnGenerated = new LongColumn(data);
                                    }else {
                                        columnGenerated = new DoubleColumn(columnValue);
                                    }
                                } catch (Exception e) {
                                    throw new IllegalArgumentException(String.format(
                                            "类型转换错误, 无法将数据[%s],类型[%s],序号[%s] 转换为[%s]", columnValue,columnType, columnIndex,
                                            "DOUBLE"));
                                }
                                break;
                            case BOOLEAN:
                                try {
                                    if ("".equals(columnValue) || columnValue == null
                                            || " ".contentEquals(columnValue) || "  ".contentEquals(columnValue))
                                    {
                                        String data = null;
                                        columnGenerated = new LongColumn(data);
                                    }else {
                                        columnGenerated = new BoolColumn(columnValue);
                                    }
                                } catch (Exception e) {
                                    throw new IllegalArgumentException(String.format(
                                            "类型转换错误, 无法将数据[%s],类型[%s],序号[%s] 转换为[%s]", columnValue,columnType, columnIndex,
                                            "BOOLEAN"));
                                }

                                break;
                            case DATE:
                                try {
                                    if (columnValue == null) {
                                        Date date = null;
                                        columnGenerated = new DateColumn(date);
                                    } else if ("".equals(columnValue)) {
                                        String data = null;
                                        columnGenerated = new LongColumn(data);
                                    } else if (" ".equals(columnValue)) {
                                        String data = null;
                                        columnGenerated = new LongColumn(data);
                                    }else{
                                        String formatString = columnConfig.getFormat();
                                        //if (null != formatString) {
                                        if (StringUtils.isNotBlank(formatString)) {
                                            // 用户自己配置的格式转换, 脏数据行为出现变化
                                            DateFormat format = columnConfig
                                                    .getDateFormat();
                                            format.setTimeZone(TimeZone.getTimeZone("GMT+0800"));
                                            columnGenerated = new DateColumn(
                                                    format.parse(columnValue));
//										columnGenerated = new StringColumn(columnValue);
                                        } else {

                                            // 框架尝试转换
                                            columnGenerated = new DateColumn(
                                                    new StringColumn(columnValue)
                                                            .asDate());
//										columnGenerated = new StringColumn(columnValue);
                                        }
                                    }
                                } catch (Exception e) {
                                    throw new IllegalArgumentException(String.format(
                                            "类型转换错误, 无法将数据[%s],类型[%s],序号[%s] 转换为[%s]", columnValue,columnType, columnIndex,
                                            "DATE"));
                                }
                                break;
                            case STRING:
                                columnGenerated = new StringColumn(columnValue);
                                break;
                            default:
                                String errorMessage = String.format(
                                        "您配置的列类型暂不支持 : 数据[%s],类型[%s],序号[%s] ", columnValue,columnType,columnIndex);
                                System.out.println(errorMessage);
                                throw DataXException
                                        .asDataXException(
                                                UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
                                                errorMessage);
                        }

                        record.addColumn(columnGenerated);

                    }
//				LOG.info(record.toString());
                    recordSender.sendToWriter(record);
                } catch (IllegalArgumentException iae) {
                    taskPluginCollector
                            .collectDirtyRecord(record, iae.getMessage());
                } catch (IndexOutOfBoundsException ioe) {
                    taskPluginCollector
                            .collectDirtyRecord(record, ioe.getMessage());
                } catch (Exception e) {
                    if (e instanceof DataXException) {
                        throw (DataXException) e;
                    }
                    // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
                    taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                }
            }

            return record;
        }






    }





    private enum Type {
        STRING, LONG, BOOLEAN, DOUBLE, DATE, ;
    }

}
