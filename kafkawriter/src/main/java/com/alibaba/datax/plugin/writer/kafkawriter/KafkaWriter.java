package com.alibaba.datax.plugin.writer.kafkawriter;

import ch.qos.logback.classic.Logger;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class KafkaWriter extends Writer {


    private static Logger logger = (Logger) LoggerFactory.getLogger("KafkaWriter");

    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static Properties props = new Properties();

    private static String kerberos = null;
    private static String principal;
    private static String userKeyTab;
    private static String krb5Conf;
    private static String jassConfFilePath;
    private static String confFilePath;


    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            logger.info("KafkaWriter");

            //            System.setProperty("java.security.krb5.conf",
//                    Thread.currentThread().getContextClassLoader().getResource("krb5.conf").getPath());
//            //加载本地jass.conf文件
//            System.setProperty("java.security.auth.login.config",
//                    Thread.currentThread().getContextClassLoader().getResource("jaas.conf").getPath());
//
//


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

            String serivializerClass = this.originalConfig.getString(Constant.SERIALIZER_CLASS,
                    Constant.SERIALIZER_CLASS_TEMP_TO);
            if (!Strings.isNullOrEmpty(serivializerClass)) {
                props.put(Constant.SERIALIZER_CLASS_TEMP, serivializerClass);
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

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;

        private List<TopicPartition> topicsAndPartitions = new ArrayList<TopicPartition>();
        private List<String> topics = new ArrayList<String>();
        private static long errorCount = 0;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            logger.info("KafkaReader task init");
            List<Configuration> topicsTemp;
            topicsTemp = this.writerSliceConfig.getListConfiguration(Constant.TOPICS);
            this.topics.clear();
            this.topicsAndPartitions.clear();
            for (Configuration obj : topicsTemp) {
                logger.info("kafka writer add topic " + obj.toString() + " partition ");
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

        }

        @Override
        public void prepare() {

        }

        public void startWrite(RecordReceiver recordReceiver) {

            logger.info("KafkaReader start write ");
//            KafkaReader<String, String> producer;

            //实例化一个生产者
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

            //死循环不停的从broker中拿数据
            long dataNumber = 0;
            while (true) {
                String line = readOneTransportRecord(recordReceiver,this.writerSliceConfig,this.getTaskPluginCollector());
                logger.info("得到的line为" + line);
                //发送topic和partition
                for (TopicPartition topic : this.topicsAndPartitions) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<String, String>(topic.topic(), topic.partition(),
                                    null, line);
                    dataNumber++;
                    if (dataNumber == 10000000000L) {
                        dataNumber = 0;
                    }
                    producer.send(record);
                }

                //发送topic仅仅String
                for (String topic : this.topics) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<String, String>(topic, line);
                    dataNumber++;

                    if (dataNumber == 10000000000L) {
                        dataNumber = 0;
                    }
                    producer.send(record);
                }
                producer.flush();


                //等待
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                    logger.error(e.getMessage());
//                }

            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }





        public String readOneTransportRecord(RecordReceiver lineReceiver, Configuration config,
                                       TaskPluginCollector taskPluginCollector) {
            char fieldDelimiter = config.getChar(Constant.FIELD_DELIMITER);
            String result = null;
            List<Configuration> columns = config.getListConfiguration(Constant.COLUMN);
            try {
                Record record = null;
                if ((record = lineReceiver.getFromReader()) != null) {
                    MutablePair<Text, Boolean> transportResult = transportOneRecord(record, fieldDelimiter, columns, taskPluginCollector);
                    if (!transportResult.getRight()) {
                        result = transportResult.left.toString();
                        System.out.println("【格式化结果】" + result);
                    }
                }
            } catch (Exception e) {
                String message = String.format("发生解析异常[%s],请检查！", e.getMessage());
                logger.error(message);
                throw DataXException.asDataXException(KafkaWriterErrorCode.Write_FILE_IO_ERROR, e);
            }
            return result;
        }


        public static MutablePair<Text, Boolean> transportOneRecord(Record record,
                                                                    char fieldDelimiter,
                                                                    List<Configuration> columnsConfiguration,
                                                                    TaskPluginCollector taskPluginCollector) {
//          LOG.info("columnsConfiguration:" + columnsConfiguration.size());
            MutablePair<List<Object>, Boolean> transportResultListTemp = transportOneRecord(record, columnsConfiguration, taskPluginCollector);
            //保存<转换后的数据,是否是脏数据>
            MutablePair<Text, Boolean> transportResult = new MutablePair<Text, Boolean>();
            transportResult.setRight(false);
            if (null != transportResultListTemp) {
                Text recordResult = new Text(StringUtils.join(transportResultListTemp.getLeft(), fieldDelimiter));
                transportResult.setRight(transportResultListTemp.getRight());
                transportResult.setLeft(recordResult);
            }
            return transportResult;
        }


        public static MutablePair<List<Object>, Boolean> transportOneRecord(
                Record record, List<Configuration> columnsConfiguration,
                TaskPluginCollector taskPluginCollector) {

            MutablePair<List<Object>, Boolean> transportResult = new MutablePair<List<Object>, Boolean>();
            transportResult.setRight(false);
            List<Object> recordList = Lists.newArrayList();
            int recordLength = 0;

            //首先数据列数由hdfswriter这边配置的列数目确定，如果没有配置，读取reader的列数
            if (columnsConfiguration != null && columnsConfiguration.size() > 0) {
                recordLength = columnsConfiguration.size();
            } else {
                recordLength = record.getColumnNumber();
            }

            if (0 != recordLength) {
                Column column;
                for (int i = 0; i < recordLength; i++) {

                    //record越界,少于的情况
                    try {
                        Column columnTest = record.getColumn(i);
                    } catch (IndexOutOfBoundsException e) {
                        //由hdfswriter决定行数
                        if (recordLength == columnsConfiguration.size()) {
                            recordList.add((new StringColumn(null)).asString());
                            //由reader决定行数
                        } else {
                            throw DataXException
                                    .asDataXException(
                                            KafkaWriterErrorCode.ERROR_DATA_ERROR,
                                            String.format(
                                                    "原文件脏数据. 数组越界 请检查源文件,或则重新配置."));
                        }
                        continue;
                    }
                    column = record.getColumn(i);
                    //todo as method
                    if (column != null && null != column.getRawData()) {
                        String rowData = column.getRawData().toString();

                        //columnsConfiguration.size()决定行数越界时
                        try {
                            Configuration conf = columnsConfiguration.get(i);
                        } catch (IndexOutOfBoundsException e) {
                            errorCount++;
                            if (errorCount > 1 && errorCount % 500000 == 1) {
                                String message = String.format("源文件有脏数据发生越界异常,数目达到:%s,此处写入前:%d字段,跳过该异常！",
                                        errorCount, i);
                                System.out.println(message);
                            }
                            continue;
                        }

                        SupportKafkaDataType columnType = SupportKafkaDataType.valueOf(
                                columnsConfiguration.get(i).getString(Constant.TYPE).toUpperCase());
                        //根据writer端类型配置做类型转换
                        try {
                            switch (columnType) {
                                case TINYINT:
                                    recordList.add(Byte.valueOf(rowData));
                                    break;
                                case SMALLINT:
                                    recordList.add(Short.valueOf(rowData));
                                    break;
                                case INT:
                                    recordList.add(Integer.valueOf(rowData));
                                    break;
                                case BIGINT:
                                case LONG:
                                    recordList.add(column.asLong());
                                    break;
                                case FLOAT:
                                    recordList.add(Float.valueOf(rowData));
                                    break;
                                case DOUBLE:
                                    recordList.add(column.asDouble());
                                    break;
                                case STRING:
                                case VARCHAR:
                                case CHAR:
                                    recordList.add(column.asString());
                                    break;
                                case BOOLEAN:
                                    recordList.add(column.asBoolean());
                                    break;
                                case DATE:
                                    recordList.add(new java.sql.Date(column.asDate().getTime()));
                                    break;
                                case TIMESTAMP:
                                    recordList.add(new java.sql.Timestamp(column.asDate().getTime()));
                                    break;
                                default:
                                    throw DataXException
                                            .asDataXException(
                                                    KafkaWriterErrorCode.ILLEGAL_VALUE,
                                                    String.format(
                                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                                            columnsConfiguration.get(i).getString(Constant.NAME),
                                                            columnsConfiguration.get(i).getString(Constant.TYPE)));
                            }
                        } catch (Exception e) {
                            // warn: 此处认为脏数据
                            String message = String.format(
                                    "字段类型转换错误：你目标字段为[%s]类型，实际字段值为[%s].",
                                    columnsConfiguration.get(i).getString(Constant.TYPE), column.getRawData().toString());
                            taskPluginCollector.collectDirtyRecord(record, message);
                            transportResult.setRight(true);
                            break;
                        }
                    } else {
                        // warn: it's all ok if nullFormat is null
                        recordList.add(null);
                    }
                }
            }
            transportResult.setLeft(recordList);
            return transportResult;
        }


    }

}
