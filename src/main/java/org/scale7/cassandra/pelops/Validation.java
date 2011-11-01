package org.scale7.cassandra.pelops;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CounterColumn;
import org.scale7.cassandra.pelops.exceptions.ModelException;

public class Validation {

    public static ByteBuffer safeGetRowKey(Bytes rowKey) {
    	if (rowKey == null || rowKey.isNull())
    		throw new ModelException("Row Key is null");
    	return rowKey.getBytes();
    }

    public static List<Bytes> validateRowKeys(List<Bytes> rowKeys) {
    	for (Bytes b : rowKeys)
    		validateRowKey(b);
    	return rowKeys;
    }

    public static List<String> validateRowKeysUtf8(List<String> rowKeys) {
    	for (String s : rowKeys)
    		if (s == null)
    			throw new ModelException("Row key is null");
    	return rowKeys;
    }

    public static Bytes validateRowKey(Bytes rowKey) {
    	if (rowKey == null || rowKey.isNull())
    		throw new ModelException("Row Key is null");
    	return rowKey;
    }

    public static void validateColumn(Column column) {
    	if (!column.isSetName())
    		throw new ModelException("Column name is null");
    	if (!column.isSetValue())
    		throw new ModelException("Column value is null");
	}

    public static void validateColumn(CounterColumn column) {
    	if (!column.isSetName())
    		throw new ModelException("Column name is null");
    	if (!column.isSetValue())
    		throw new ModelException("Column value is null");
	}

    public static void validateColumns(List<Column> columns) {
    	for (Column c : columns) validateColumn(c);
	}

    public static void validateCounterColumns(List<CounterColumn> columns) {
    	for (CounterColumn c : columns) validateColumn(c);
	}

    public static void validateColumnNames(List<Bytes> names) {
    	for (Bytes n : names) validateColumnName(n);
	}

    public static void validateColumnName(Bytes name) {
		if (name == null || name.isNull())
			throw new ModelException("Column name is null");
	}
}
