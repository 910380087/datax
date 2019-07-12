package com.alibaba.datax.plugin.reader.redisreader;

public class Constant {

    public static final String BOOTSTRAP_SERVERS_TEMP="bootstrap.servers";
    public static final String ACKS_TEMP="acks";
    public static final String RETRIES_TEMP="retries";
    public static final String BATCH_SIZE_TEMP="batch.size";
    public static final String LINGER_MS_TEMP="linger.ms";
    public static final String BUFFER_MEMORY_TEMP="buffer.memory";
    public static final String KEY_SERIALIZER_TEMP="org.apache.kafka.common.serialization.StringSerializer";
    public static final String KEY_SERIALIZER_TEMP_DE="org.apache.kafka.common.serialization.StringDeserializer";
    public static final String KEY_SERIALIZER_TEMP_TO="key.serializer";
    public static final String KEY_SERIALIZER_TEMP_TO_DE="key.deserializer";
    public static final String VALUE_SERIALIZER_TEMP_DE="org.apache.kafka.common.serialization.StringDeserializer";
    public static final String VALUE_SERIALIZER_TEMP="org.apache.kafka.common.serialization.StringSerializer";
    public static final String VALUE_SERIALIZER_TEMP_TO="value.serializer";
    public static final String VALUE_SERIALIZER_TEMP_TO_DE="value.deserializer";
    public static final String GROUPID_TEMP="group.id";
    public static final String AUTOCOMMIT_TEMP="enable.auto.commit";
    public static final String FORM_BEGINNING="auto.offset.reset";
    public static final String FORM_BEGINNING_TEMP="formBeginning";
    public static final String AUTOCOMMITINTERVAL_TEMP="auto.commit.interval.ms";

    public static final String GROUPID="groupId";
    public static final String AUTOCOMMIT="autoCommit";
    public static final String AUTOCOMMITINTERVAL="autoCommitInterval";
    public static final String TOPICS="topics";



    public static final String MODE="mode";
    public static final String MODE_SINGLE="single";
    public static final String MODE_CLUSTER="cluster";
    public static final String MODE_SENTINEL="sentinel";


    public static final String PRINCIPAL="principal";
    public static final String USER_KEYTAB="userKeytab";
    public static final String KRB5_CONF="krb5Conf";
    public static final String JASS_CONF="jassConfFilePath";
    public static final String CONF_FILE="configFilePath";


    public static final String BOOTSTRAP_SERVERS="servers";
    public static final String PASSWORD="password";



    public static final String HASH_KEY="hashKey";
    public static final String LIST_KEY="listKey";
    public static final String SET_KEY="setKey";



    public static final String RETRIES="retries";
    public static final String BATCH_SIZE="batchSize";
    public static final String LINGER_MS="delayMs";
    public static final String BUFFER_MEMORY="bufferMemory";
    public static final String KEY_SERIALIZER="keySerializer";
    public static final String KEY_DESERIALIZER="keyDeserializer";
    public static final String VALUE_DESERIALIZER="valueDeserializer";
    public static final String VALUE_SERIALIZER="valueSerializer";
    public static final String PARTITION="partition";
    public static final String STOP_DATE="stopDate";
    public static final String STOP_NUMBER="stopNumber";
    public static final Integer PARTITION_DEFAULT= -19191919;


}
