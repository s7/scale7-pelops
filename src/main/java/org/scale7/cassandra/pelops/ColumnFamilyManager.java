/*
 * The MIT License
 *
 * Copyright (c) 2011 Dominic Williams, Daniel Washusen and contributors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.Cassandra.AsyncClient;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.system_add_column_family_call;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.system_drop_column_family_call;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.system_update_column_family_call;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.truncate_call;
import org.apache.cassandra.thrift.CfDef;
import org.apache.thrift.async.AsyncMethodCallback;

public class ColumnFamilyManager extends ManagerOperand {

    public static final String CFDEF_TYPE_STANDARD = "Standard";
    public static final String CFDEF_TYPE_SUPER = "Super";

    public static final String CFDEF_COMPARATOR_BYTES = "BytesType";
    public static final String CFDEF_COMPARATOR_ASCII = "AsciiType";
    public static final String CFDEF_COMPARATOR_UTF8 = "UTF8Type";
    public static final String CFDEF_COMPARATOR_LONG = "LongType";
    public static final String CFDEF_COMPARATOR_LEXICAL_UUID = "LexicalUUIDType";
    public static final String CFDEF_COMPARATOR_TIME_UUID = "TimeUUIDType";
    public static final String CFDEF_COMPARATOR_INTEGER = "IntegerType";

    public static final String CFDEF_VALIDATION_CLASS_COUNTER = "CounterColumnType";

    public ColumnFamilyManager(Cluster cluster, String keyspace) {
        super(cluster, keyspace);
    }

    public void truncateColumnFamily(final String columnFamily) throws Exception {
    	IManagerOperation<truncate_call, Void> operation = new IManagerOperation<truncate_call, Void>() {

            @Override
            public void execute(AsyncClient conn, AsyncMethodCallback<truncate_call> callback)
                    throws Exception {
                conn.truncate(columnFamily, callback);
            }

            @Override
            public Void getResult(truncate_call call) throws Exception {
                call.getResult();
                return null;
            }
        };
        tryOperation(operation);
    }

    public String addColumnFamily(final CfDef columnFamilyDefinition) throws Exception {
    	IManagerOperation<system_add_column_family_call, String> operation = new IManagerOperation<system_add_column_family_call, String>() {

            @Override
            public void execute(AsyncClient conn, AsyncMethodCallback<system_add_column_family_call> callback)
                    throws Exception {
                conn.system_add_column_family(columnFamilyDefinition, callback);
            }

            @Override
            public String getResult(system_add_column_family_call call)
                    throws Exception {
                return call.getResult();
            }
        };
        return tryOperation(operation);
    }

    public String updateColumnFamily(final CfDef columnFamilyDefinition) throws Exception {
    	IManagerOperation<system_update_column_family_call, String> operation = new IManagerOperation<system_update_column_family_call, String>() {

            @Override
            public void execute(AsyncClient conn, AsyncMethodCallback<system_update_column_family_call> callback)
                    throws Exception {
                conn.system_update_column_family(columnFamilyDefinition, callback);
            }

            @Override
            public String getResult(system_update_column_family_call call)
                    throws Exception {
                return call.getResult();
            }
        };
        return tryOperation(operation);
    }

    public String dropColumnFamily(final String columnFamily) throws Exception {
    	IManagerOperation<system_drop_column_family_call, String> operation = new IManagerOperation<system_drop_column_family_call, String>() {

            @Override
            public void execute(AsyncClient conn, AsyncMethodCallback<system_drop_column_family_call> callback)
                    throws Exception {
                conn.system_drop_column_family(columnFamily, callback);
            }

            @Override
            public String getResult(system_drop_column_family_call call)
                    throws Exception {
                return call.getResult();
            }
        };
        return tryOperation(operation);
    }

    /* - https://issues.apache.org/jira/browse/CASSANDRA-1630
    public String renameColumnFamily(final String oldName, final String newName) throws Exception {
    	IManagerOperation<system_rename_column_family_call, String> operation = new IManagerOperation<system_rename_column_family_call, String>() {

            @Override
            public void execute(AsyncClient conn, AsyncMethodCallback<system_rename_column_family_call> callback)
                    throws Exception {
                conn.system_rename_column_family(oldName, newName, callback);
            }

            @Override
            public String getResult(system_rename_column_family_call call)
                    throws Exception {
                return call.getResult();
            }
        };
        return tryOperation(operation);
    }
    */
}