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

import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.orc.TypeDescription;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONWriter;

/**
 * 
 * @author Ashish (ashu.impetus@gmail.com)
 *
 */
public class JsonFieldFiller {

    public static void processRow(JSONWriter writer, VectorizedRowBatch batch,
            TypeDescription schema, int row) throws JSONException {
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

    static void setValue(JSONWriter writer, ColumnVector vector,
            TypeDescription schema, int row) throws JSONException {
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
                writer.value(((DecimalColumnVector) vector).vector[row]
                        .toString());
                break;
            case DATE:
                writer.value(new DateWritable(
                        (int) ((LongColumnVector) vector).vector[row])
                        .toString());
                break;
            case TIMESTAMP:
                writer.value(((TimestampColumnVector) vector)
                        .asScratchTimestamp(row).toString());
                break;
            case LIST:
                setList(writer, (ListColumnVector) vector, schema, row);
                break;
            case STRUCT:
                setStruct(writer, (StructColumnVector) vector, schema, row);
                break;
            case UNION:
                setUnion(writer, (UnionColumnVector) vector, schema, row);
                break;
            case BINARY:
                // To prevent similar mistakes like the on described in https://github.com/pinterest/secor/pull/1018,
                // it would be better to explicitly throw an exception here rather than ignore the incoming values,
                // which causes silent failures in a later stage.
                throw new UnsupportedOperationException();
            case MAP:
                setMap(writer, (MapColumnVector) vector, schema, row);
                break;
            default:
                throw new IllegalArgumentException("Unknown type "
                        + schema.toString());
            }
        } else {
            writer.value(null);
        }
    }

    private static void setList(JSONWriter writer, ListColumnVector vector,
            TypeDescription schema, int row) throws JSONException {
        writer.array();
        int offset = (int) vector.offsets[row];
        TypeDescription childType = schema.getChildren().get(0);
        for (int i = 0; i < vector.lengths[row]; ++i) {
            setValue(writer, vector.child, childType, offset + i);
        }
        writer.endArray();
    }

    private static void setStruct(JSONWriter writer, StructColumnVector batch,
            TypeDescription schema, int row) throws JSONException {
        writer.object();
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> fieldTypes = schema.getChildren();
        for (int i = 0; i < fieldTypes.size(); ++i) {
            writer.key(fieldNames.get(i));
            setValue(writer, batch.fields[i], fieldTypes.get(i), row);
        }
        writer.endObject();
    }

    private static void setMap(JSONWriter writer, MapColumnVector vector,
                               TypeDescription schema, int row) throws JSONException {
        writer.object();
        List<TypeDescription> schemaChildren = schema.getChildren();
        BytesColumnVector keyVector = (BytesColumnVector) vector.keys;
        long length = vector.lengths[row];
        long offset = vector.offsets[row];
        for (int i = 0; i < length; i++) {
            writer.key(keyVector.toString((int) offset + i));
            setValue(writer, vector.values, schemaChildren.get(1), (int) offset + i);
        }
        writer.endObject();
    }

    /**
     * Writes a single row of union type as a JSON object.
     *
     * @throws JSONException
     */
    private static void setUnion(JSONWriter writer, UnionColumnVector vector,
                                 TypeDescription schema, int row) throws JSONException {
        int tag = vector.tags[row];
        List<TypeDescription> schemaChildren = schema.getChildren();
        ColumnVector columnVector = vector.fields[tag];
        setValue(writer, columnVector, schemaChildren.get(tag), row);
    }
}
