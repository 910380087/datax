package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum RedisWriterErrorCode  implements ErrorCode {
    CONFIG_INVALID_EXCEPTION("RedisWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("RedisWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("RedisWriter-02", "您填写的参数值不合法."),
    WRITER_FILE_WITH_CHARSET_ERROR("RedisWriter-03", "您配置的编码未能正常写入."),
    Write_FILE_IO_ERROR("RedisWriter-04", "您配置的文件在写入时出现IO异常."),
    WRITER_RUNTIME_EXCEPTION("RedisWriter-05", "出现运行时异常, 请联系我们."),
    CONNECT_HDFS_IO_ERROR("RedisWriter-06", "与HDFS建立连接时出现IO异常."),
    COLUMN_REQUIRED_VALUE("RedisWriter-07", "您column配置中缺失了必须填写的参数值."),
    HDFS_RENAME_FILE_ERROR("RedisWriter-08", "将文件移动到配置路径失败."),
    KERBEROS_LOGIN_ERROR("RedisWriter-09", "KERBEROS认证失败"),
    CONFIG_INVALID_CLUSTER_EXCEPTION("redisReader-00", "您的参数配置错误,主机地址为必填参数,应填写为hostname:port,hostname:port,hostname:port."),
    ERROR_DATA_ERROR("RedisWriter-10", "源文件脏数据错误");

    private final String code;
    private final String description;

    private RedisWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}
