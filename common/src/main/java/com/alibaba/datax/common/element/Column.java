package com.alibaba.datax.common.element;

import com.alibaba.fastjson.JSON;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 */
public abstract class Column {

	private Type type;

	private Object rawData;

	private int byteSize;

	public Column(final Object object, final Type type, int byteSize) {
		this.rawData = object;
		this.type = type;
		//bytesize加上分隔符的byte 最后一个不变,因为是还有个换行符
		this.byteSize = byteSize;
	}

	public Object getRawData() {
		return this.rawData;
	}

	public Type getType() {
		return this.type;
	}

	public int getByteSize() {
		return this.byteSize;
	}

	protected void setType(Type type) {
		this.type = type;
	}

	protected void setRawData(Object rawData) {
		this.rawData = rawData;
	}

	protected void setByteSize(int byteSize) {
		//bytesize加上分隔符的byte 最后一个不变,因为是还有个换行符
		this.byteSize = byteSize;
	}

	public abstract Long asLong();

	public abstract Double asDouble();

	public abstract String asString();

	public abstract Date asDate();

	public abstract byte[] asBytes();

	public abstract Boolean asBoolean();

	public abstract BigDecimal asBigDecimal();

	public abstract BigInteger asBigInteger();

//	@Override
//	public String toString() {
//		return JSON.toJSONString(this);
//	}

	@Override
	public String toString() {
		return "{" +
				"类型=" + type +
				", 数据=" + rawData +
				", 字节数=" + byteSize +
				'}';
	}

	public enum Type {
		BAD, NULL, INT, LONG, DOUBLE, STRING, BOOL, DATE, BYTES
	}
}
