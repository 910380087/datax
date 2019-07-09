package com.alibaba.datax.plugin.reader.kafkareader;

import ch.qos.logback.classic.Logger;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.google.common.base.Strings;
import main.java.com.alibaba.datax.plugin.writer.kafkareader.Constant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class KafkaReader extends Reader {



    private static Logger logger = (Logger) LoggerFactory.getLogger("KafkaReader");


    private static Properties props = new Properties();


    public static class Job extends Reader.Job {
        private Configuration originalConfig = null;


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            logger.info("KafkaReader");
            // check protection
            String bootstrapServers = this.originalConfig.getNecessaryValue(Constant.BOOTSTRAP_SERVERS,
                    DBUtilErrorCode.CONF_ERROR);
            if (!Strings.isNullOrEmpty(bootstrapServers)){
                props.put(Constant.BOOTSTRAP_SERVERS_TEMP,bootstrapServers);
            }
            String acks = this.originalConfig.getString(Constant.ACKS,
                    "all");
            if (!Strings.isNullOrEmpty(bootstrapServers)){
                props.put(Constant.ACKS_TEMP,acks);
            }
            Integer retries = this.originalConfig.getInt(Constant.RETRIES,
                    0);
            if (retries > 0){
                props.put(Constant.RETRIES_TEMP,retries);
            }else {
                props.put(Constant.RETRIES_TEMP, 0);
            }
            Integer batchSize = this.originalConfig.getInt(Constant.BATCH_SIZE,
                    16834);
            props.put(Constant.RETRIES_TEMP,batchSize);

            Integer lingerMs = this.originalConfig.getInt(Constant.LINGER_MS,
                    1);
            props.put(Constant.LINGER_MS_TEMP,lingerMs);

            Long bufferMemory = this.originalConfig.getLong(Constant.BUFFER_MEMORY,
                    33554432);
            props.put(Constant.BUFFER_MEMORY_TEMP,bufferMemory);

            String keySerializer = this.originalConfig.getString(Constant.KEY_SERIALIZER,
                    Constant.KEY_SERIALIZER_TEMP);
            if (!Strings.isNullOrEmpty(keySerializer)){
                props.put(Constant.KEY_SERIALIZER_TEMP,keySerializer);
            }
            String valueSerializer = this.originalConfig.getString(Constant.VALUE_SERIALIZER,
                    Constant.VALUE_SERIALIZER_TEMP);
            if (!Strings.isNullOrEmpty(valueSerializer)){
                props.put(Constant.VALUE_SERIALIZER_TEMP,valueSerializer);
            }

            String groupId = this.originalConfig.getNecessaryValue(Constant.GROUPID,
                    DBUtilErrorCode.CONF_ERROR);
            if (!Strings.isNullOrEmpty(groupId)){
                props.put(Constant.GROUPID_TEMP,groupId);
            }
            String enabelAutoCommit = this.originalConfig.getString(Constant.AUTOCOMMIT,
                    "true");
            if (!Strings.isNullOrEmpty(enabelAutoCommit)){
                props.put(Constant.AUTOCOMMIT_TEMP,enabelAutoCommit);
            }
            String autoCommitInterval = this.originalConfig.getString(Constant.AUTOCOMMITINTERVAL,
                    "50");
            if (!Strings.isNullOrEmpty(autoCommitInterval)){
                props.put(Constant.AUTOCOMMITINTERVAL_TEMP,autoCommitInterval);
            }



            logger.info("kafkaReader init success");
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
        private Configuration writerSliceConfig;

        private List<String> topics = new ArrayList<String>();

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            logger.info("KafkaReader task init");
            List<Object> topicsTemp = new ArrayList<Object>();
            topicsTemp = this.writerSliceConfig.getList(Constant.TOPICS);
            this.topics.clear();
            for (Object obj: topicsTemp){
                this.topics.add(obj.toString());
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
            logger.info("KafkaReader startRead ");
//            KafkaReader<String, String> producer;

            //实例化一个消费者
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
            //消费者订阅主题，可以订阅多个主题
//            consumer.subscribe(Arrays.asList("mytopic1"));
            consumer.subscribe(topics);
            //死循环不停的从broker中拿数据
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
//                UnstructuredStorageReaderUtil.transportOneRecord(recordSender,
//                        column, parseRows, nullFormat, this.getTaskPluginCollector());

//                UnstructuredStorageReaderUtil.transportOneRecord(RecordSender recordSender,
//                        Configuration configuration,
//                        TaskPluginCollector taskPluginCollector,
//                        String line);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }

        }
    }

}
