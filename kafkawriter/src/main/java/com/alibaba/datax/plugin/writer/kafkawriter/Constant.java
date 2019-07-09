package com.alibaba.datax.plugin.writer.kafkawriter;

public class Constant {

    public static final String BOOTSTRAP_SERVERS_TEMP="bootstrap.servers";
    public static final String ACKS_TEMP="acks";
    public static final String RETRIES_TEMP="retries";
    public static final String BATCH_SIZE_TEMP="batch.size";
    public static final String LINGER_MS_TEMP="linger.ms";
    public static final String BUFFER_MEMORY_TEMP="buffer.memory";
    public static final String KEY_SERIALIZER_TEMP="org.apache.kafka.common.serialization.IntegerSerializer";
    public static final String VALUE_SERIALIZER_TEMP="org.apache.kafka.common.serialization.StringSerializer";
    public static final String GROUPID_TEMP="group.id";
    public static final String AUTOCOMMIT_TEMP="enable.auto.commit";
    public static final String AUTOCOMMITINTERVAL_TEMP="auto.commit.interval.ms";

    public static final String BOOTSTRAP_SERVERS="servers";
    public static final String GROUPID="groupId";
    public static final String AUTOCOMMIT="autoCommit";
    public static final String AUTOCOMMITINTERVAL="autoCommitInterval";
    public static final String ACKS="acks";
    public static final String RETRIES="retries";
    public static final String BATCH_SIZE="batchSize";
    public static final String LINGER_MS="delayMs";
    public static final String BUFFER_MEMORY="bufferMemory";
    public static final String KEY_SERIALIZER="keySerializer";
    public static final String VALUE_SERIALIZER="valueSerializer";


}
