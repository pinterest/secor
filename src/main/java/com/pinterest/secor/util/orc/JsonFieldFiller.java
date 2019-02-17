/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pinterest.secor.util.orc;

import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.orc.TypeDescription;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONWriter;

import java.util.List;

/**
 * @author Ashish (ashu.impetus@gmail.com)
 */
public class JsonFieldFiller {

    public static void processRow(JSONWriter writer, VectorizedRowBatch batch, TypeDescription schema, int row)
            throws JSONException {
        if (schema.getCategory() == TypeDescription.Category.STRUCT) {
            List<TypeDescription> fieldTypes = schema.getChildren();
            List<String> fieldNames = schema.getFieldNames();
            writer.object();
            for (int c = 0; c < batch.cols.length; ++c) {
                writer.key(fieldNames.get(c));
                setValue(writer, batch.cols[c], fieldTypes.get(c), row);
            }
            writer.endObject();
        } else {
            setValue(writer, batch.cols[0], schema, row);
        }
    }

    static void setValue(JSONWriter writer, ColumnVector vector, TypeDescription schema, int row) throws JSONException {
        if (vector.isRepeating) {
            row = 0;
        }
        if (vector.noNulls || !vector.isNull[row]) {
            switch (schema.getCategory()) {
                case BOOLEAN:
                    writer.value(((LongColumnVector) vector).vector[row] != 0);
                    break;
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                    writer.value(((LongColumnVector) vector).vector[row]);
                    break;
                case FLOAT:
                case DOUBLE:
                    writer.value(((DoubleColumnVector) vector).vector[row]);
                    break;
                case STRING:
                case CHAR:
                case VARCHAR:
                    writer.value(((BytesColumnVector) vector).toString(row));
                    break;
                case DECIMAL:
                    writer.value(((DecimalColumnVector) vector).vector[row].toString());
                    break;
                case DATE:
                    writer.value(new DateWritable((int) ((LongColumnVector) vector).vector[row]).toString());
                    break;
                case TIMESTAMP:
                    writer.value(((TimestampColumnVector) vector).asScratchTimestamp(row).toString());
                    break;
                case LIST:
                    setList(writer, (ListColumnVector) vector, schema, row);
                    break;
                case STRUCT:
                    setStruct(writer, (StructColumnVector) vector, schema, row);
                    break;
                case UNION:
                    // printUnion(writer, (UnionColumnVector) vector, schema, row);
                    break;
                case BINARY:
                    // printBinary(writer, (BytesColumnVector) vector, row);
                    break;
                case MAP:
                    // printMap(writer, (MapColumnVector) vector, schema, row);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + schema.toString());
            }
        } else {
            writer.value(null);
        }
    }

    private static void setList(JSONWriter writer, ListColumnVector vector, TypeDescription schema, int row)
            throws JSONException {
        writer.array();
        int offset = (int) vector.offsets[row];
        TypeDescription childType = schema.getChildren().get(0);
        for (int i = 0; i < vector.lengths[row]; ++i) {
            setValue(writer, vector.child, childType, offset + i);
        }
        writer.endArray();
    }

    private static void setStruct(JSONWriter writer, StructColumnVector batch, TypeDescription schema, int row)
            throws JSONException {
        writer.object();
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> fieldTypes = schema.getChildren();
        for (int i = 0; i < fieldTypes.size(); ++i) {
            writer.key(fieldNames.get(i));
            setValue(writer, batch.fields[i], fieldTypes.get(i), row);
        }
        writer.endObject();
    }
}
