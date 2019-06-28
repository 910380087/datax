package com.alibaba.datax.core.statistics.plugin.task.util;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson.JSON;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DirtyRecord implements Record {
	private List<Column> columns = new ArrayList<Column>();
	private List<DirtyColumn> dirtyColumns = new ArrayList<DirtyColumn>();

	public static DirtyRecord asDirtyRecord(final Record record) {
		DirtyRecord result = new DirtyRecord();
		for (int i = 0; i < record.getColumnNumber(); i++) {
			result.addColumn(DirtyColumn.asDirtyColumn(record.getColumn(i),i));
		}

		return result;
	}

	@Override
	public void addColumn(Column column) {
		this.columns.add(
                DirtyColumn.asDirtyColumn(column, this.columns.size()));
	}

	public void addDirtyColumn(Column column, int index) {
		this.dirtyColumns.add(
				DirtyColumn.asNewDirtyColumn(column, index));
	}

	@Override
	public String toString() {
		return JSON.toJSONString(this.columns);
	}

	@Override
	public void setColumn(int i, Column column) {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public Column getColumn(int i) {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public int getColumnNumber() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public int getByteSize() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public int getMemorySize() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

}

class DirtyColumn extends Column {
	private int index;

	public static Column asDirtyColumn(final Column column, int index) {
		return new DirtyColumn(column, index);
	}

	public static DirtyColumn asNewDirtyColumn(final Column column, int index) {
		return new DirtyColumn(column, index);
	}

	private DirtyColumn(Column column, int index) {
		this(null == column ? null : column.getRawData(),
				null == column ? Column.Type.NULL : column.getType(),
				null == column ? 0 : column.getByteSize(), index);
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public String getMayError(){
		String mayError = "";
		switch (super.getType()){
			case BAD:
				mayError += "没有配置类型或错误类型";
				break;
			case INT:
				mayError += "类型最大值:" + new Integer(Integer.MAX_VALUE);
				break;
			case DATE:
				mayError += "日期格式等";
				break;
			case LONG:
				mayError += "类型最大值:" + new Long(Long.MAX_VALUE);
				break;
			default:
				break;
		}
		return mayError;
	}

	@Override
	public Long asLong() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public Double asDouble() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public String asString() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public Date asDate() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public byte[] asBytes() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public Boolean asBoolean() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public BigDecimal asBigDecimal() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	@Override
	public BigInteger asBigInteger() {
		throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
				"该方法不支持!");
	}

	private DirtyColumn(Object object, Type type, int byteSize, int index) {
		super(object, type, byteSize);
		this.setIndex(index);
	}

	@Override
	public String toString() {
		return "{" +
				"类型=" + super.getType() +
				", 数据=" + super.getRawData() +
				", 字节数=" + super.getByteSize() +
				", 序号=" + getIndex() +
				'}';
	}
}
