package com.pinterest.secor.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.pinterest.secor.common.SecorConfig;

public class OrcUtil {

    private static final Logger LOG = LoggerFactory.getLogger(OrcUtil.class);
    public static final String COMMA_SEPARATOR = ",";
    public static final byte[] SEPARATOR_BYTES = new byte[]{'\u0001', '\u0002', '\u0003'};
    public static final String SEPARATOR_STR = new String(SEPARATOR_BYTES);
    public static final String SPACE = " ";

    private TypeDescription schema;
    private String schemaMapfile;

    public OrcUtil(SecorConfig config) {
        schemaMapfile = config.getOrcSchemaMapFile();
    }

    public TypeDescription getOrcSchema() {
        return schema == null? (schema = readFileForSchema(new File(schemaMapfile))):schema;
    }

    private TypeDescription readFileForSchema(File schemaMapFile) {

        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(schemaMapfile)));
            StringBuilder builder = new StringBuilder();
            String line;
            boolean first = true;
            builder.append("struct<");
            while ((line = reader.readLine()) != null) {
                if (!first)
                    builder.append(COMMA_SEPARATOR);
                builder.append(line.trim());
                first = false;
            }
            builder.append(">");
            reader.close();
            schema = TypeDescription.fromString(builder.toString());
        } catch (IOException e) {
            LOG.error("Unable to read schema for Orc due to {}, Orc conversion cannot work", e);
            Throwables.propagate(e);
        }
        return schema;
    }

    public void convertFromProtoToOrcType(Message protoMessage, VectorizedRowBatch rowBatch, int row) {
        List<FieldDescriptor> fields = protoMessage.getDescriptorForType().getFields();
        for (int i=0; i<fields.size(); i++) {
            FieldDescriptor field = fields.get(i);
            switch(field.getJavaType()) {
            case BOOLEAN:
                LongColumnVector col = (LongColumnVector) rowBatch.cols[i];
                col.vector[row] = ((Boolean)protoMessage.getField(field))?0:1;
                break;
            case BYTE_STRING:
            case STRING:
                BytesColumnVector bCol = (BytesColumnVector) rowBatch.cols[i];
                bCol.setVal(row, protoMessage.getField(field).toString().getBytes());
                break;
            case DOUBLE:
                DoubleColumnVector dCol = (DoubleColumnVector) rowBatch.cols[i];
                dCol.vector[row] = (Double)protoMessage.getField(field);
                break;
            case FLOAT:
                dCol = (DoubleColumnVector) rowBatch.cols[i];
                dCol.vector[row] = (Float)protoMessage.getField(field);
                break;
            case INT:
                col = (LongColumnVector) rowBatch.cols[i];
                col.vector[row] = (Integer)protoMessage.getField(field);
                break;
            case LONG:
                col = (LongColumnVector) rowBatch.cols[i];
                col.vector[row] = (Long)protoMessage.getField(field);
                break;
            default:
                Throwables.propagate(new IllegalArgumentException("Datatype not supported "+ field.getJavaType().name()));
            }
        }
    }

    public void convertFromOrcToProtoType(Message protoMessage, VectorizedRowBatch rowBatch, int row) {
        List<FieldDescriptor> fields = protoMessage.getDescriptorForType().getFields();
        Message.Builder builder = protoMessage.toBuilder();
        for (int i=0; i<fields.size(); i++) {
            FieldDescriptor field = fields.get(i);
            switch(field.getJavaType()) {
            case BOOLEAN:
                LongColumnVector col = (LongColumnVector) rowBatch.cols[i];
                builder.setField(field, (col.vector[row] == 0)?true:false);
                break;
            case BYTE_STRING:
            case STRING:
                BytesColumnVector bCol = (BytesColumnVector) rowBatch.cols[i];
                builder.setField(field, bCol.toString(row));
                break;
            case DOUBLE:
                DoubleColumnVector dCol = (DoubleColumnVector) rowBatch.cols[i];
                builder.setField(field, dCol.vector[row]);
                break;
            case FLOAT:
                dCol = (DoubleColumnVector) rowBatch.cols[i];
                builder.setField(field, Float.parseFloat(Double.toString(dCol.vector[row])));
                break;
            case INT:
                col = (LongColumnVector) rowBatch.cols[i];
                builder.setField(field, Integer.parseInt(((Long)col.vector[row]).toString()));
                break;
            case LONG:
                col = (LongColumnVector) rowBatch.cols[i];
                builder.setField(field, (Long)col.vector[row]);
                break;
            default:
                Throwables.propagate(new IllegalArgumentException("Datatype not supported "+ field.getJavaType().name()));
            }
        }
    }

}
