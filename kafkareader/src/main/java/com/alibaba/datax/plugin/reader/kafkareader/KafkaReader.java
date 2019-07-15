package com.alibaba.datax.plugin.reader.kafkareader;

import ch.qos.logback.classic.Logger;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

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


public class KafkaReader extends Reader {


    private static Logger logger = (Logger) LoggerFactory.getLogger("KafkaReader");
    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static Properties props = new Properties();

    private static String kerberos = null;
    private static String principal;
    private static String userKeyTab;
    private static String krb5Conf;
    private static String jassConfFilePath;
    private static String confFilePath;

    private static Lock lock = new ReentrantLock();
    private static long totalDataNumber = 0;


    public static long getTotalDataNumber() {
        return totalDataNumber;
    }

    public static void setTotalDataNumber(long totalDataNumber) {
        KafkaReader.totalDataNumber = totalDataNumber;
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
            logger.info("KafkaReader");
//            System.setProperty("java.security.krb5.conf",
//                    Thread.currentThread().getContextClassLoader().getResource("krb5.conf").getPath());
//            //加载本地jass.conf文件
//            System.setProperty("java.security.auth.login.config",
//                    Thread.currentThread().getContextClassLoader().getResource("jaas.conf").getPath());
//
//
            KafkaReader.setTotalDataNumber(0);

//            security.protocol=SASL_PLAINTEXT
//            sasl.mechanism=GSSAPI
//            sasl.kerberos.service.name=kafka
//
//
//            security.protocol=SASL_PLAINTEXT
//            sasl.mechanism=GSSAPI
//            sasl.kerberos.service.name=kafka
//            group.id=test-consumer-group

            //加载临时jass.conf
        /* File jaasConf = KerberosUtils.configureJAAS(Thread.currentThread().getContextClassLoader()
                .getResource("wms_dev.keytab").getPath(), "wms_dev@WONHIGH.COM");
            System.setProperty("java.security.auth.login.config", jaasConf.getAbsolutePath());*/
//            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            //System.setProperty("sun.security.krb5.debug","true");

            //kerberos init
            kerberos = this.originalConfig.getNecessaryValue(Constant.KERBEROS,
                    DBUtilErrorCode.CONF_ERROR);
            if (!Strings.isNullOrEmpty(kerberos) && "true".equals(kerberos)) {
                logger.info("KafkaReader kerberos init");
                principal = this.originalConfig.getNecessaryValue(Constant.PRINCIPAL,
                        DBUtilErrorCode.CONF_ERROR);
                if (Strings.isNullOrEmpty(principal)) {
                    logger.error("KafkaReader kerberos principal is null");
                }

                jassConfFilePath = this.originalConfig.getNecessaryValue(Constant.JASS_CONF,
                        DBUtilErrorCode.CONF_ERROR);
                if (Strings.isNullOrEmpty(jassConfFilePath)) {
                    logger.error("KafkaReader kerberos jassConf is null");
                }


                confFilePath = this.originalConfig.getNecessaryValue(Constant.CONF_FILE,
                        DBUtilErrorCode.CONF_ERROR);
                if (Strings.isNullOrEmpty(confFilePath)) {
                    logger.error("KafkaReader kerberos config file is null");
                }

                userKeyTab = this.originalConfig.getNecessaryValue(Constant.USER_KEYTAB,
                        DBUtilErrorCode.CONF_ERROR);
                if (Strings.isNullOrEmpty(userKeyTab)) {
                    logger.error("KafkaReader kerberos userKeyTab is null");
                }

                krb5Conf = this.originalConfig.getNecessaryValue(Constant.KRB5_CONF,
                        DBUtilErrorCode.CONF_ERROR);
                if (Strings.isNullOrEmpty(krb5Conf)) {
                    logger.error("KafkaReader kerberos krb5Conf is null");
                }


                try {
                    kerberosAuth(principal, userKeyTab, krb5Conf, jassConfFilePath, confFilePath);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("KafkaReader kerberos 认证登陆异常" + e.getMessage());
                }
            }


            // check protection
            String bootstrapServers = this.originalConfig.getNecessaryValue(Constant.BOOTSTRAP_SERVERS,
                    DBUtilErrorCode.CONF_ERROR);
            if (!Strings.isNullOrEmpty(bootstrapServers)) {
                props.put(Constant.BOOTSTRAP_SERVERS_TEMP, bootstrapServers);
            }


            String acks = this.originalConfig.getString(Constant.ACKS,
                    "all");
            if (!Strings.isNullOrEmpty(bootstrapServers)) {
                props.put(Constant.ACKS_TEMP, acks);
            }
            Integer retries = this.originalConfig.getInt(Constant.RETRIES,
                    0);
            if (retries > 0) {
                props.put(Constant.RETRIES_TEMP, retries);
            } else {
                props.put(Constant.RETRIES_TEMP, 0);
            }
            Integer batchSize = this.originalConfig.getInt(Constant.BATCH_SIZE,
                    16834);
            props.put(Constant.RETRIES_TEMP, batchSize);

            Integer lingerMs = this.originalConfig.getInt(Constant.LINGER_MS,
                    1);
            props.put(Constant.LINGER_MS_TEMP, lingerMs);

            Long bufferMemory = this.originalConfig.getLong(Constant.BUFFER_MEMORY,
                    33554432);
            props.put(Constant.BUFFER_MEMORY_TEMP, bufferMemory);

            String keySerializer = this.originalConfig.getString(Constant.KEY_SERIALIZER,
                    Constant.KEY_SERIALIZER_TEMP);
            if (!Strings.isNullOrEmpty(keySerializer)) {
                props.put(Constant.KEY_SERIALIZER_TEMP_TO, keySerializer);
            }
            String valueSerializer = this.originalConfig.getString(Constant.VALUE_SERIALIZER,
                    Constant.VALUE_SERIALIZER_TEMP);
            if (!Strings.isNullOrEmpty(valueSerializer)) {
                props.put(Constant.VALUE_SERIALIZER_TEMP_TO, valueSerializer);
            }

            String valueDeserializer = this.originalConfig.getString(Constant.VALUE_DESERIALIZER,
                    Constant.VALUE_SERIALIZER_TEMP_DE);
            if (!Strings.isNullOrEmpty(valueDeserializer)) {
                props.put(Constant.VALUE_SERIALIZER_TEMP_TO_DE, valueDeserializer);
            }

            String keyDeserializer = this.originalConfig.getString(Constant.KEY_DESERIALIZER,
                    Constant.KEY_SERIALIZER_TEMP_DE);
            if (!Strings.isNullOrEmpty(keyDeserializer)) {
                props.put(Constant.KEY_SERIALIZER_TEMP_TO_DE, keyDeserializer);
            }

            String groupId = this.originalConfig.getNecessaryValue(Constant.GROUPID,
                    DBUtilErrorCode.CONF_ERROR);
            if (!Strings.isNullOrEmpty(groupId)) {
                props.put(Constant.GROUPID_TEMP, groupId);
            }
            String enabelAutoCommit = this.originalConfig.getString(Constant.AUTOCOMMIT,
                    "true");
            if (!Strings.isNullOrEmpty(enabelAutoCommit)) {
                props.put(Constant.AUTOCOMMIT_TEMP, enabelAutoCommit);
            }
            String autoCommitInterval = this.originalConfig.getString(Constant.AUTOCOMMITINTERVAL,
                    "50");
            if (!Strings.isNullOrEmpty(autoCommitInterval)) {
                props.put(Constant.AUTOCOMMITINTERVAL_TEMP, autoCommitInterval);
            }

            String fromBegining = this.originalConfig.getString(Constant.FORM_BEGINNING_TEMP,
                    "earliest");
            if (!Strings.isNullOrEmpty(fromBegining)) {
                props.put(Constant.FORM_BEGINNING, fromBegining);
            }

            logger.info("kafkaReader init success");
        }


        public void kerberosAuth(String kerberosPrincipal, String userKeyTab, String krb5Conf,
                                 String jassConfFile, String confFilePath) throws Exception {
            logger.info("kerberos auth");
            logger.info("kerberos auth principal :" + kerberosPrincipal + " user.kertab : " + userKeyTab + " krb5.conf : " + krb5Conf);
            org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

            hadoopConf.set("hadoop.security.authentication", "kerberos");


            try {
                //获取classpath
                Properties propertiesTemp = new Properties();
                // 使用InPutStream流读取properties文件
                BufferedReader bufferedReader = new BufferedReader(new FileReader(confFilePath));
                propertiesTemp.load(bufferedReader);
                logger.info("获取配置文件成功");
                logger.info("遍历添加配置文件");
                Set<Map.Entry<Object, Object>> entrySet = propertiesTemp.entrySet();//返回的属性键值对实体
                for (Map.Entry<Object, Object> entry : entrySet) {
                    System.setProperty(entry.getKey().toString(), entry.getValue().toString());
                    props.setProperty(entry.getKey().toString(), entry.getValue().toString());
                    System.out.println(entry.getKey() + "=" + entry.getValue());
                }
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("获取配置文件错误");
                logger.error(e.getMessage());
            }

            System.setProperty("java.security.auth.login.config", jassConfFile);
            System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5Conf);

            String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
            if (ret == null) {
                logger.error(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
                throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
            }
            if (!ret.equals(krb5Conf)) {
                logger.error(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5Conf + ".");
                throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5Conf + ".");
            }

            kerberosAuthentication(kerberosPrincipal, userKeyTab);
        }

        private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath) {
            if (StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
                logger.info(kerberosPrincipal);
                logger.info(kerberosKeytabFilePath);
                try {
                    logger.info("kerberos认证登陆");
                    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
                    logger.info("kerberos认证通过");
                } catch (Exception e) {
                    String message = String.format("kerberos认证失败,krb5Conf[%s]和kerberosPrincipal[%s]填写正确",
                            kerberosKeytabFilePath, kerberosPrincipal);
                    logger.error(message);
                    throw DataXException.asDataXException(new ErrorCode() {
                        @Override
                        public String getCode() {
                            return "kafka-01";
                        }

                        @Override
                        public String getDescription() {
                            return "KERBEROS认证失败";
                        }
                    }, e);
                }
            }
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

        private List<TopicPartition> topicsAndPartitions = new ArrayList<TopicPartition>();
        private List<String> topics = new ArrayList<String>();

        private ExecutorService executorService = Executors.newFixedThreadPool(10);

        private String stopDate;
        private Long stopNumber;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            logger.info("KafkaReader task init");
            List<Configuration> topicsTemp;
            topicsTemp = this.readerSliceConfig.getListConfiguration(Constant.TOPICS);
            this.topics.clear();
            for (Configuration obj : topicsTemp) {
                logger.info("KafkaReader add topic and partition : " + obj.toString());
                int part = obj.getInt("partition", Constant.PARTITION_DEFAULT);
                String topic = obj.getString("topic", null);
                if (topic == null) {
                    throw DataXException.asDataXException(new ErrorCode() {
                                                              @Override
                                                              public String getCode() {
                                                                  return "kafka-01";
                                                              }

                                                              @Override
                                                              public String getDescription() {
                                                                  return "未填写topic";
                                                              }
                                                          },
                            String.format("您提供配置topic有误，[%s]是必填参数.", "topic"));
                }
                if (part == Constant.PARTITION_DEFAULT) {
                    this.topics.add(topic);
                } else {
                    TopicPartition topicPartition = new TopicPartition(obj.getString("topic"),
                            part);
                    this.topicsAndPartitions.add(topicPartition);
                }
            }


            this.stopDate = this.readerSliceConfig.getString(Constant.STOP_DATE,
                    null);
            this.stopNumber = this.readerSliceConfig.getLong(Constant.STOP_NUMBER,
                    -1);
            logger.info("stop kafka reader 数据量:" + this.stopNumber);
            logger.info("stop kafka reader 时间:" + this.stopDate);
            if (!Strings.isNullOrEmpty(stopDate) && stopNumber == -1) {
                throw DataXException.asDataXException(new ErrorCode() {
                                                          @Override
                                                          public String getCode() {
                                                              return "kafka-02";
                                                          }

                                                          @Override
                                                          public String getDescription() {
                                                              return "请填写kafareader停止时间，" +
                                                                      "或者填写kafka停止抽取停止数目";
                                                          }
                                                      },
                        String.format("您提供配置kafka停止条件有误，[%s]是必填参数.", "stopDate或stopNumber"));
            }


        }

        @Override
        public void prepare() {
            logger.info("KafkaReader task prepare");
        }

        @Override
        public void post() {
            logger.info("KafkaReader task post");
        }

        @Override
        public void destroy() {
            logger.info("KafkaReader task destroy");
        }


        @Override
        public void startRead(RecordSender recordSender) {
            logger.info("【KafkaReader】 startRead ");
            logger.info("【KafkaReader】 topics size: " + topics.size() + "  topics and partitions  size: " + topicsAndPartitions.size());

            //仅仅topic的执行器
            if (topics.size() > 0) {
                logger.info("创建只有topics的kafka reader");
                KafkaConsumerRunner topicsRunner =
                        new KafkaConsumerRunner(props, null,
                                topics, this.stopDate, this.stopNumber,recordSender,this.getTaskPluginCollector(),
                                this.readerSliceConfig);
                executorService.submit(topicsRunner);
            }

            //topic和partition的执行器
            if (topicsAndPartitions.size() > 0) {
                logger.info("创建有topics和partitions的kafka reader");
                KafkaConsumerRunner topciAndPartitionRunner =
                        new KafkaConsumerRunner(props, topicsAndPartitions,
                                null, this.stopDate, this.stopNumber,recordSender,this.getTaskPluginCollector(),
                                this.readerSliceConfig);
                executorService.submit(topciAndPartitionRunner);
            }
            //没有topic
            if(topicsAndPartitions.size() == 0 && topics.size() == 0) {
                logger.info("没有topics直接退出kafka reader");
                return;
            }

            logger.info("添加线程池执行kafka抽取");


            //循环判断是否可以结束
            while (true){
//                boolean isDateAfter = afterCurrentDate(stopDate);
//                boolean isDataNumberAfter = KafkaReader.getTotalDataNumber() >= this.stopNumber;
//                logger.info("当前:totalDataNumber=" + KafkaReader.getTotalDataNumber()
//                + "  停止stopNumber=" + this.stopNumber
//                + " 日期是否到了" + isDateAfter + " 数目是否到了 " + isDataNumberAfter);

                if (this.stopNumber != -1) {
                    if (KafkaReader.getTotalDataNumber() >= this.stopNumber) {
                        executorService.shutdownNow();
                        logger.info("数目达到停止");
                        return;
                    } else if (afterCurrentDateTask(stopDate)) {
                        logger.info("时间达到停止");
                        executorService.shutdownNow();
                        return;
                    }
                }
            }


        }




        public boolean afterCurrentDateTask(String stopDate){
            if (null == stopDate) return false;
            //格式化日期
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
            Date dateSet = null;
            try {
                dateSet = simpleDateFormat.parse(stopDate);
            } catch (ParseException e) {
                e.printStackTrace();
                throw DataXException.asDataXException(new ErrorCode() {
                                                          @Override
                                                          public String getCode() {
                                                              return "kafka-11";
                                                          }

                                                          @Override
                                                          public String getDescription() {
                                                              return "停止日期不正确Topics";
                                                          }
                                                      },
                        String.format("您提供配置停止日期格式有误，正确格式为yyyy-mm-dd HH:mm:ss，[%s].", e.getMessage()));
            }
            if(dateSet != null){
                if (dateSet.getTime() <= (new Date()).getTime()) {
                    return true;
                }else {
                    return false;
                }
            }else {
                return false;
            }
        }




    }


    /**
     * KafkaConsumerRunner kafka 消费则执行线程
     */
    public static class KafkaConsumerRunner implements Callable<Boolean> {

        private Properties propertiesTopic = new Properties();
        private Properties propertiesTopicAndPartition = new Properties();
        private List<TopicPartition> topicsAndPartitions;
        private List<String> topics;
        private String stopDateRunner;
        private Long stopNumberRunner = 0L;


        private RecordSender recordSender;
        private TaskPluginCollector taskPluginCollector;
        private Configuration readerSliceConfig;

        //构造函数
        public KafkaConsumerRunner(Properties properties,
                                   List<TopicPartition> topicsAndPartitions,
                                   List<String> topics,
                                   String stopDate,
                                   Long stopNumber,
                                   RecordSender recordSender,
                                   TaskPluginCollector taskPluginCollector,
                                   Configuration readerSliceConfig) {
            Set<Map.Entry<Object, Object>> entrySet = properties.entrySet();//返回的属性键值对实体
            for (Map.Entry<Object, Object> entry : entrySet) {
                this.propertiesTopic.setProperty(entry.getKey().toString(), entry.getValue().toString());
                this.propertiesTopicAndPartition.setProperty(entry.getKey().toString(), entry.getValue().toString());
            }
            this.topicsAndPartitions = topicsAndPartitions;
            this.topics = topics;
            this.stopDateRunner = stopDate;
            this.stopNumberRunner = stopNumber;
            this.recordSender = recordSender;
            this.taskPluginCollector = taskPluginCollector;
            this.readerSliceConfig = readerSliceConfig;
            System.out.println("[Topic or Topic and partition Thread] KafkaConsumerRunner初始化完成!");
        }


        //执行函数
        @Override
        public Boolean call() throws Exception {
            System.out.println("[Topic or Topic and partition Thread] 消费线程开始执行!");
            //实例化一个消费者
            KafkaConsumer<String, String> consumerTopic = new KafkaConsumer<String, String>(this.propertiesTopic);
            KafkaConsumer<String, String> consumerTopicAndPartition =
                    new KafkaConsumer<String, String>(this.propertiesTopicAndPartition);

            //消费者订阅主题，可以订阅多个主题
            if (null != topics && topics.size() > 0) {
                Thread.currentThread().setName("[Topic Thread]");
                consumerTopic.subscribe(topics);
                while (true) {
                    ConsumerRecords<String, String> records = consumerTopic.poll(100);
                    delay(1000);
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(String.format("【读取值】value = %s%n", record.value()));
                        KafkaReader.increaseTotalDataNumber();
                        //解析发送给reader
                        String kafkaReadValue = record.value();
                        transportOneLineToRecord(kafkaReadValue,this.recordSender,this.taskPluginCollector,
                                this.readerSliceConfig);
                        //检测是否停止
                        if (isReturn(stopNumberRunner, stopDateRunner)) {
//                            System.exit(0);
                            return true;
                        }
                    }
//                    System.out.println(Thread.currentThread().getName() + " data number: " + KafkaReader.getTotalDataNumber() + " stop data number: "
//                            + stopNumberRunner);
                }


            } else if (null != topicsAndPartitions && topicsAndPartitions.size() > 0) {
                Thread.currentThread().setName("[Topic and partition Thread]");
                //死循环不停的从broker中拿数据
                while (true) {
                    ConsumerRecords<String, String> records = consumerTopicAndPartition.poll(100);
                    delay(1000);
                    for (ConsumerRecord<String, String> record : records) {
//                        System.out.println(String.format("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
                        KafkaReader.increaseTotalDataNumber();
                        //解析发送给reader
                        String kafkaReadValue = record.value();
                        transportOneLineToRecord(kafkaReadValue,this.recordSender,this.taskPluginCollector,
                                this.readerSliceConfig);
                        //检测是否停止
                        if (isReturn(stopNumberRunner, stopDateRunner)) {
//                            System.exit(0);
                            return true;
                        }
                    }
                }
            } else {
                return false;
            }



        }


        public boolean isReturn(long stopNumber, String stopDate) {
            boolean isDataNumberAfter = false;
            if (-1 == stopNumber){
                isDataNumberAfter = false;
            }else{
                isDataNumberAfter = KafkaReader.getTotalDataNumber() >= stopNumber;
            }
            if (isDataNumberAfter) {
                boolean isDateAfter = afterCurrentDate(stopDate);
                logger.info("当前:totalDataNumber=" + KafkaReader.getTotalDataNumber()
                        + "  停止stopNumber=" + stopNumber
                        + " 日期是否到了" + isDateAfter + " 数目是否到了 " + isDataNumberAfter);
                logger.info("数目达到停止");
                return true;
            } else if (afterCurrentDate(stopDate)) {
                boolean isDateAfter = afterCurrentDate(stopDate);
                logger.info("当前:totalDataNumber=" + KafkaReader.getTotalDataNumber()
                        + "  停止stopNumber=" + stopNumber
                        + " 日期是否到了" + isDateAfter + " 数目是否到了 " + isDataNumberAfter);
                logger.info("时间达到停止");
                return true;
            } else {
                return false;
            }
        }


        //计算时间是否到期
        public boolean afterCurrentDate(String stopDate) {
            if (null == stopDate) return false;
            //格式化日期
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
            Date dateSet = null;
            try {
                dateSet = simpleDateFormat.parse(stopDate);
            } catch (ParseException e) {
                e.printStackTrace();
                throw DataXException.asDataXException(new ErrorCode() {
                                                          @Override
                                                          public String getCode() {
                                                              return "kafka-11";
                                                          }

                                                          @Override
                                                          public String getDescription() {
                                                              return "停止日期不正确Topics";
                                                          }
                                                      },
                        String.format("您提供配置停止日期格式有误，正确格式为yyyy-mm-dd HH:mm:ss，[%s].", e.getMessage()));
            }
            if (dateSet != null) {
                if (dateSet.getTime() <= (new Date()).getTime()) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        //延时工具
        public void delay(int ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
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
            List<ColumnEntry> column = UnstructuredStorageReaderUtil
                    .getListColumnEntry(readerSliceConfig, Key.COLUMN);
            String delimiterInStr = readerSliceConfig
                    .getString(Key.FIELD_DELIMITER);
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
