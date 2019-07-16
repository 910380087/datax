package com.alibaba.datax.plugin.reader.mqreader;

import ch.qos.logback.classic.Logger;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.LoggerFactory;
import org.springframework.jca.cci.core.RecordCreator;

import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class MqReader extends Reader {


    private static Logger logger = (Logger) LoggerFactory.getLogger("MqReader");

    private static String servers = null;
    private static String password = null;
    private static String username = null;
    private static String group = null;
    private static String pattern = null;
    private static String topic = null;


    private static Lock lock = new ReentrantLock();


    public static class Job extends Reader.Job {
        private Configuration originalConfig = null;


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            logger.info("MqReader");
            //connection init
            servers = this.originalConfig.getNecessaryValue(Constant.BOOTSTRAP_SERVERS,
                    DBUtilErrorCode.CONF_ERROR);
            if (Strings.isNullOrEmpty(servers)) {
                logger.error("mqreader server host and port must not be null");
                throw DataXException.asDataXException(MqReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        new RuntimeException());
            }

            topic = this.originalConfig.getNecessaryValue(Constant.TOPIC,
                    DBUtilErrorCode.CONF_ERROR);
            if (!Strings.isNullOrEmpty(topic) ) {
                logger.info("mqreader topic is null ");
            }

            pattern = this.originalConfig.getString(Constant.PATTERN,
                    "*");
            username = this.originalConfig.getString(Constant.USERNAME,
                    null);
            password = this.originalConfig.getString(Constant.PASSWORD,
                    null);
            group = this.originalConfig.getString(Constant.GROUP,
                    null);

            logger.info("mqreader init success");
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
        private BlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            logger.info("mqreader task init");
        }

        @Override
        public void prepare() {
            logger.info("mqreader task prepare");
        }

        @Override
        public void post() {
            logger.info("mqreader task post");
        }

        @Override
        public void destroy() {
            logger.info("mqreader task destroy");
        }


        @Override
        public void startRead(final RecordSender recordSender) {
            logger.info("【mqreader】 startRead ");

            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        //设置消费者组
                        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);

                        consumer.setVipChannelEnabled(false);
                        consumer.setNamesrvAddr(servers);
                        //设置消费者端消息拉取策略，表示从哪里开始消费
                        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

                        //设置消费者拉取消息的策略，*表示消费该topic下的所有消息，也可以指定tag进行消息过滤
                        consumer.subscribe(topic, pattern);

                        //消费者端启动消息监听，一旦生产者发送消息被监听到，就打印消息，和rabbitmq中的handlerDelivery类似
//                        consumer.registerMessageListener(new MqMessageListener(this.getTaskPluginCollector(),recordSender,this.readerSliceConfig));

                        consumer.registerMessageListener(new MessageListenerOrderly() {
                            @Override
                            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                                Iterator<MessageExt> iterator = list.iterator();
                                while (iterator.hasNext()){
                                    MessageExt msgExt = iterator.next();
//                                    String topic = msgExt.getTopic();
//                                    String tag = msgExt.getTags();
                                    String msg = new String(msgExt.getBody());
                                    try {
                                        queue.put(msg);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
//                                    System.out.println("消费响应：msgId : " + msgExt.getMsgId() + ",  msgBody : " + msg + ", tag:" + tag + ", topic:" + topic);
                                }
                                return ConsumeOrderlyStatus.SUCCESS;
                            }
                        });
                        consumer.start();
                        logger.info("Consumer Started....");
                    }catch (Exception e){
                        e.printStackTrace();
                        logger.error(e.getMessage());
                    }
                }
            });

            thread.setName("生产线程");
            thread.start();

            try {
                String line = null;
                while ((line = queue.poll(10, TimeUnit.SECONDS)) != null){
//                    Record record = recordSender.createRecord();
//                    record.addColumn(new StringColumn("测试信息123"));
//                    System.out.println("测试信息:" +recordSender.toString());
//                    recordSender.sendToWriter(record);
                    transportOneLineToRecord(line,recordSender,
                            getTaskPluginCollector(),readerSliceConfig);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }

        }


        public static class MqMessageListener implements MessageListenerConcurrently{

            private TaskPluginCollector taskPluginCollector;
            private RecordSender recordSender;
            private Configuration readerSliceConfig;



            public MqMessageListener( final TaskPluginCollector taskPluginCollector,
                     final RecordSender recordSender,
                     final Configuration readerSliceConfig) {
                this.taskPluginCollector = taskPluginCollector;
                this.recordSender = recordSender;
                System.out.println("哈哈MqMessageListener:" +recordSender.toString());
                this.readerSliceConfig = readerSliceConfig;
            }

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                Iterator<MessageExt> iterator = list.iterator();
                while (iterator.hasNext()){
                    MessageExt msgExt = iterator.next();
                    String topic = msgExt.getTopic();
                    String tag = msgExt.getTags();
                    String msg = new String(msgExt.getBody());
                    Record record = this.recordSender.createRecord();
                    record.addColumn(new StringColumn("哈哈11111"));
                    this.recordSender.sendToWriter(record);
//                    transportOneLineToRecord(msg,this.recordSender,
//                            this.taskPluginCollector,this.readerSliceConfig);
                    System.out.println("消费响应：msgId : " + msgExt.getMsgId() + ",  msgBody : " + msg + ", tag:" + tag + ", topic:" + topic);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
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
//				    LOG.info(record.toString());
//                  System.out.println("发送的record:" + record.toString());
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
