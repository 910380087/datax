package com.alibaba.datax.plugin.reader.gbase.error;

import com.alibaba.datax.common.spi.ErrorCode;

public class GbaseError implements ErrorCode {


    private String msg;
    private String code;


    public GbaseError( String code,String msg) {
        this.msg = msg;
        this.code = code;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return msg;
    }
}
