package com.alibaba.datax.plugin.writer.mqwriter;

public class Constant {

    public static final String BOOTSTRAP_SERVERS="servers";

    public static final String PASSWORD="password";
    public static final String USERNAME="username";
    public static final String GROUP="group";
    public static final String TAG="tag";
    public static final String PATTERN="pattern";
    public static final String TOPIC="topic";

    public static final String TOPICS="topics";


    // must have
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    // not must, default UTF-8
    public static final String ENCODING = "encoding";
    //not must
    public static final String SPLIT_LINE ="splitLine";

    // must have for column
    public static final String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String DATE_FORMAT = "dateFormat";


}
