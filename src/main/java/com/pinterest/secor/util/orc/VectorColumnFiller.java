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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.pinterest.secor.util.BackOffUtil;
import org.apache.hadoop.hive.common.type.HiveDecimal;
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
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Ashish (ashu.impetus@gmail.com)
 *
 */
public class VectorColumnFiller {
    private static final Logger LOG = LoggerFactory.getLogger(VectorColumnFiller.class);

    public interface JsonConverter {
        void convert(JsonElement value, ColumnVector vect, int row);
    }

    static class BooleanColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                LongColumnVector vector = (LongColumnVector) vect;
                vector.vector[row] = value.getAsBoolean() ? 1 : 0;
            }
        }
    }

    static class LongColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                LongColumnVector vector = (LongColumnVector) vect;
                vector.vector[row] = value.getAsLong();
            }
        }
    }

    static class DoubleColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                DoubleColumnVector vector = (DoubleColumnVector) vect;
                vector.vector[row] = value.getAsDouble();
            }
        }
    }

    static class StringColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                BytesColumnVector vector = (BytesColumnVector) vect;
                byte[] bytes = value.getAsString().getBytes(
                        StandardCharsets.UTF_8);
                vector.setRef(row, bytes, 0, bytes.length);
            }
        }
    }

    static class BinaryColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                BytesColumnVector vector = (BytesColumnVector) vect;
                String binStr = value.getAsString();
                byte[] bytes = new byte[binStr.length() / 2];
                for (int i = 0; i < bytes.length; ++i) {
                    bytes[i] = (byte) Integer.parseInt(
                            binStr.substring(i * 2, i * 2 + 2), 16);
                }
                vector.setRef(row, bytes, 0, bytes.length);
            }
        }
    }

    static class TimestampColumnConverter implements JsonConverter {
        BackOffUtil back = new BackOffUtil(true);

        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                if (value.getAsJsonPrimitive().isString()) {
                    TimestampColumnVector vector = (TimestampColumnVector) vect;
                    vector.set(
                            row,
                            Timestamp.valueOf(value.getAsString().replaceAll(
                                    "[TZ]", " ")));
                } else if (value.getAsJsonPrimitive().isNumber()) {
                    TimestampColumnVector vector = (TimestampColumnVector) vect;
                    vector.set(
                            row,
                            new Timestamp(value.getAsLong()));
                } else {
                    if (!back.isBackOff()) {
                        LOG.warn("Timestamp is neither string nor number: {}", value);
                    }
                    vect.noNulls = false;
                    vect.isNull[row] = true;
                }
            }
        }
    }

    static class DecimalColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                DecimalColumnVector vector = (DecimalColumnVector) vect;
                vector.vector[row].set(HiveDecimal.create(value.getAsString()));
            }
        }
    }

    static class MapColumnConverter implements JsonConverter {
        private JsonConverter[] childConverters;

        public MapColumnConverter(TypeDescription schema) {
            assertKeyType(schema);

            List<TypeDescription> childTypes = schema.getChildren();
            childConverters = new JsonConverter[childTypes.size()];
            for (int c = 0; c < childConverters.length; ++c) {
                childConverters[c] = createConverter(childTypes.get(c));
            }
        }

        /**
         * Rejects non-string keys. This is a limitation imposed by JSON specifications that only allows strings
         * as keys.
         */
        private void assertKeyType(TypeDescription schema) {
            // NOTE: It may be tempting to ensure that schema.getChildren() returns at least one child here, but the
            // validity of an ORC schema is ensured by TypeDescription. Malformed ORC schema could be a concern.
            // For example, an ORC schema of `map<>` may produce a TypeDescription instance with no child. However,
            // TypeDescription.fromString() rejects any malformed ORC schema and therefore we may assume only valid
            // ORC schema will make to this point.
            TypeDescription keyType = schema.getChildren().get(0);
            String keyTypeName = keyType.getCategory().getName();
            if (!keyTypeName.equalsIgnoreCase("string")) {
                throw new IllegalArgumentException(
                        String.format("Unsupported key type: %s", keyTypeName));
            }
        }

        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                MapColumnVector vector = (MapColumnVector) vect;
                JsonObject obj = value.getAsJsonObject();
                vector.lengths[row] = obj.size();
                vector.offsets[row] = row > 0 ? vector.offsets[row - 1] + vector.lengths[row - 1] : 0;

                // Ensure enough space is available to store the keys and the values
                vector.keys.ensureSize((int) vector.offsets[row] + obj.size(), true);
                vector.values.ensureSize((int) vector.offsets[row] + obj.size(), true);

                int i = 0;
                for (String key : obj.keySet()) {
                    childConverters[0].convert(new JsonPrimitive(key), vector.keys, (int) vector.offsets[row] + i);
                    childConverters[1].convert(obj.get(key), vector.values, (int) vector.offsets[row] + i);
                    i++;
                }
            }
        }
    }

    static class UnionColumnConverter implements JsonConverter {

        /**
         * Union type in ORC is essentially a collection of two or more non-compatible types,
         * and it is represented by multiple child columns under UnionColumnVector.
         * Thus we need their converters.
         */
        private Map<String, JsonConverter> childConverters = new HashMap<>();

        public UnionColumnConverter(TypeDescription schema) {
            List<TypeDescription> children = schema.getChildren();
            for (TypeDescription childType : children) {
                JsonConverter converter = createConverter(childType);
                childConverters.put(childType.getCategory().getName(), converter);
            }
        }

        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else if (value.isJsonPrimitive()) {
                UnionColumnVector vector = (UnionColumnVector) vect;
                JsonPrimitive primitive = value.getAsJsonPrimitive();

                // TODO: We need to figure out the type of primitive...
                if (primitive.isNumber()) {
                    JsonConverter converter = childConverters.get("int");
                    converter.convert(value, vector.fields[0], row);
                }
                else if (primitive.isBoolean()) {
                    throw new NotImplementedException();
                }
                else if (primitive.isString()) {
                    JsonConverter converter = childConverters.get("string");
                    converter.convert(value, vector.fields[1], row);
                }
            } else {
                throw new NotImplementedException();
            }
        }
    }

    static class StructColumnConverter implements JsonConverter {
        private JsonConverter[] childrenConverters;
        private List<String> fieldNames;

        public StructColumnConverter(TypeDescription schema) {
            List<TypeDescription> kids = schema.getChildren();
            childrenConverters = new JsonConverter[kids.size()];
            for (int c = 0; c < childrenConverters.length; ++c) {
                childrenConverters[c] = createConverter(kids.get(c));
            }
            fieldNames = schema.getFieldNames();
        }

        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                StructColumnVector vector = (StructColumnVector) vect;
                JsonObject obj = value.getAsJsonObject();
                for (int c = 0; c < childrenConverters.length; ++c) {
                    JsonElement elem = obj.get(fieldNames.get(c));
                    childrenConverters[c].convert(elem, vector.fields[c], row);
                }
            }
        }
    }

    static class ListColumnConverter implements JsonConverter {
        private JsonConverter childrenConverter;

        public ListColumnConverter(TypeDescription schema) {
            childrenConverter = createConverter(schema.getChildren().get(0));
        }

        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                ListColumnVector vector = (ListColumnVector) vect;
                JsonArray obj = value.getAsJsonArray();
                vector.lengths[row] = obj.size();
                vector.offsets[row] = vector.childCount;
                vector.childCount += vector.lengths[row];
                vector.child.ensureSize(vector.childCount, true);
                for (int c = 0; c < obj.size(); ++c) {
                    childrenConverter.convert(obj.get(c), vector.child,
                            (int) vector.offsets[row] + c);
                }
            }
        }
    }

    public static JsonConverter createConverter(TypeDescription schema) {
        switch (schema.getCategory()) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
            return new LongColumnConverter();
        case FLOAT:
        case DOUBLE:
            return new DoubleColumnConverter();
        case CHAR:
        case VARCHAR:
        case STRING:
            return new StringColumnConverter();
        case DECIMAL:
            return new DecimalColumnConverter();
        case TIMESTAMP:
            return new TimestampColumnConverter();
        case BINARY:
            return new BinaryColumnConverter();
        case BOOLEAN:
            return new BooleanColumnConverter();
        case STRUCT:
            return new StructColumnConverter(schema);
        case LIST:
            return new ListColumnConverter(schema);
        case MAP:
            return new MapColumnConverter(schema);
        case UNION:
            return new UnionColumnConverter(schema);
        default:
            throw new IllegalArgumentException("Unhandled type " + schema);
        }
    }

    public static void fillRow(int rowIndex, JsonConverter[] converters,
            TypeDescription schema, VectorizedRowBatch batch, JsonObject data) {
        List<String> fieldNames = schema.getFieldNames();
        for (int c = 0; c < converters.length; ++c) {
            JsonElement field = data.get(fieldNames.get(c));
            if (field == null) {
                batch.cols[c].noNulls = false;
                batch.cols[c].isNull[rowIndex] = true;
            } else {
                converters[c].convert(field, batch.cols[c], rowIndex);
            }
        }
    }
}