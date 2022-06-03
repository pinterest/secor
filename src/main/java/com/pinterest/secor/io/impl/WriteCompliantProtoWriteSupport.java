package com.pinterest.secor.io.impl;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.util.Protobufs;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.proto.ProtoSchemaConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.IncompatibleSchemaModificationException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteCompliantProtoWriteSupport<T extends MessageOrBuilder> extends WriteSupport<T> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteCompliantProtoWriteSupport.class);
    public static final String PB_CLASS_WRITE = "parquet.proto.writeClass";
    public static final String PB_SPECS_COMPLIANT_WRITE = "parquet.proto.writeSpecsCompliant";
    // force to true for now
    private boolean writeSpecsCompliant = true;
    private RecordConsumer recordConsumer;
    private Class<? extends Message> protoMessage;
    private WriteCompliantProtoWriteSupport<T>.MessageWriter messageWriter;

    public WriteCompliantProtoWriteSupport(Class<? extends Message> protobufClass) {
        this.protoMessage = protobufClass;
    }

    public String getName() {
        return "protobuf";
    }

    public static void setSchema(Configuration configuration, Class<? extends Message> protoClass) {
        configuration.setClass("parquet.proto.writeClass", protoClass, Message.class);
    }

    public static void setWriteSpecsCompliant(Configuration configuration, boolean writeSpecsCompliant) {
        configuration.setBoolean("parquet.proto.writeSpecsCompliant", writeSpecsCompliant);
    }

    public void write(T record) {
        WriteCompliantProtoWriteSupport.this.recordConsumer.startMessage();

        try {
            this.messageWriter.writeTopLevelMessage(record);
        } catch (RuntimeException var4) {
            Message m = record instanceof Builder ? ((Builder)record).build() : (Message)record;
            LOG.error("Cannot write message " + var4.getMessage() + " : " + m);
            throw var4;
        }

        WriteCompliantProtoWriteSupport.this.recordConsumer.endMessage();
    }

    public void prepareForWrite(RecordConsumer recordConsumer) {
        WriteCompliantProtoWriteSupport.this.recordConsumer = recordConsumer;
    }

    public WriteContext init(Configuration configuration) {
        if (this.protoMessage == null) {
            Class<? extends Message> pbClass = configuration.getClass("parquet.proto.writeClass", (Class)null, Message.class);
            if (pbClass == null) {
                String msg = "Protocol buffer class not specified.";
                String hint = " Please use method ProtoParquetOutputFormat.setProtobufClass(...) or other similar method.";
                throw new BadConfigurationException(msg + hint);
            }

            this.protoMessage = pbClass;
        }

        this.writeSpecsCompliant = configuration.getBoolean("parquet.proto.writeSpecsCompliant", this.writeSpecsCompliant);
        MessageType rootSchema = (new ProtoSchemaConverter(this.writeSpecsCompliant)).convert(this.protoMessage);
        Descriptor messageDescriptor = Protobufs.getMessageDescriptor(this.protoMessage);
        this.validatedMapping(messageDescriptor, rootSchema);
        this.messageWriter = new MessageWriter(messageDescriptor, rootSchema);
        Map<String, String> extraMetaData = new HashMap();
        extraMetaData.put("parquet.proto.class", this.protoMessage.getName());
        extraMetaData.put("parquet.proto.descriptor", this.serializeDescriptor(this.protoMessage));
        extraMetaData.put("parquet.proto.writeSpecsCompliant", String.valueOf(this.writeSpecsCompliant));
        return new WriteContext(rootSchema, extraMetaData);
    }

    private void validatedMapping(Descriptor descriptor, GroupType parquetSchema) {
        List<FieldDescriptor> allFields = descriptor.getFields();
        Iterator var4 = allFields.iterator();

        String fieldName;
        int fieldIndex;
        int parquetIndex;
        do {
            if (!var4.hasNext()) {
                return;
            }

            FieldDescriptor fieldDescriptor = (FieldDescriptor)var4.next();
            fieldName = fieldDescriptor.getName();
            fieldIndex = fieldDescriptor.getIndex();
            parquetIndex = parquetSchema.getFieldIndex(fieldName);
        } while(fieldIndex == parquetIndex);

        String message = "FieldIndex mismatch name=" + fieldName + ": " + fieldIndex + " != " + parquetIndex;
        throw new IncompatibleSchemaModificationException(message);
    }

    private FieldWriter unknownType(FieldDescriptor fieldDescriptor) {
        String exceptionMsg = "Unknown type with descriptor \"" + fieldDescriptor + "\" and type \"" + fieldDescriptor.getJavaType() + "\".";
        throw new InvalidRecordException(exceptionMsg);
    }

    private String serializeDescriptor(Class<? extends Message> protoClass) {
        Descriptor descriptor = Protobufs.getMessageDescriptor(protoClass);
        DescriptorProto asProto = descriptor.toProto();
        return TextFormat.printToString(asProto);
    }

    class BinaryWriter extends FieldWriter {
        BinaryWriter() {
            super();
        }

        final void writeRawValue(Object value) {
            ByteString byteString = (ByteString)value;
            Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
            WriteCompliantProtoWriteSupport.this.recordConsumer.addBinary(binary);
        }
    }

    class BooleanWriter extends FieldWriter {
        BooleanWriter() {
            super();
        }

        final void writeRawValue(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.addBoolean((Boolean)value);
        }
    }

    class EnumWriter extends FieldWriter {
        EnumWriter() {
            super();
        }

        final void writeRawValue(Object value) {
            Binary binary = Binary.fromString(((EnumValueDescriptor)value).getName());
            WriteCompliantProtoWriteSupport.this.recordConsumer.addBinary(binary);
        }
    }

    class DoubleWriter extends FieldWriter {
        DoubleWriter() {
            super();
        }

        final void writeRawValue(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.addDouble((Double)value);
        }
    }

    class FloatWriter extends FieldWriter {
        FloatWriter() {
            super();
        }

        final void writeRawValue(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.addFloat((Float)value);
        }
    }

    class MapWriter extends FieldWriter {
        private final FieldWriter keyWriter;
        private final FieldWriter valueWriter;

        public MapWriter(FieldWriter keyWriter, FieldWriter valueWriter) {
            super();
            this.keyWriter = keyWriter;
            this.valueWriter = valueWriter;
        }

        final void writeRawValue(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.startGroup();
            WriteCompliantProtoWriteSupport.this.recordConsumer.startField("key_value", 0);
            Iterator var2 = ((Collection)value).iterator();

            while(var2.hasNext()) {
                Message msg = (Message)var2.next();
                WriteCompliantProtoWriteSupport.this.recordConsumer.startGroup();
                Descriptor descriptorForType = msg.getDescriptorForType();
                FieldDescriptor keyDesc = descriptorForType.findFieldByName("key");
                FieldDescriptor valueDesc = descriptorForType.findFieldByName("value");
                this.keyWriter.writeField(msg.getField(keyDesc));
                this.valueWriter.writeField(msg.getField(valueDesc));
                WriteCompliantProtoWriteSupport.this.recordConsumer.endGroup();
            }

            WriteCompliantProtoWriteSupport.this.recordConsumer.endField("key_value", 0);
            WriteCompliantProtoWriteSupport.this.recordConsumer.endGroup();
        }
    }

    class LongWriter extends FieldWriter {
        LongWriter() {
            super();
        }

        final void writeRawValue(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.addLong((Long)value);
        }
    }

    class IntWriter extends FieldWriter {
        IntWriter() {
            super();
        }

        final void writeRawValue(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.addInteger((Integer)value);
        }
    }

    class StringWriter extends FieldWriter {
        StringWriter() {
            super();
        }

        final void writeRawValue(Object value) {
            Binary binaryString = Binary.fromString((String)value);
            WriteCompliantProtoWriteSupport.this.recordConsumer.addBinary(binaryString);
        }
    }

    class RepeatedWriter extends FieldWriter {
        final FieldWriter fieldWriter;

        RepeatedWriter(FieldWriter fieldWriter) {
            super();
            this.fieldWriter = fieldWriter;
        }

        final void writeRawValue(Object value) {
            throw new UnsupportedOperationException("Array has no raw value");
        }

        final void writeField(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.startField(this.fieldName, this.index);
            List<?> list = (List)value;
            Iterator var3 = list.iterator();

            while(var3.hasNext()) {
                Object listEntry = var3.next();
                this.fieldWriter.writeRawValue(listEntry);
            }

            WriteCompliantProtoWriteSupport.this.recordConsumer.endField(this.fieldName, this.index);
        }
    }

    class ArrayWriter extends FieldWriter {
        final FieldWriter fieldWriter;

        ArrayWriter(FieldWriter fieldWriter) {
            super();
            this.fieldWriter = fieldWriter;
        }

        final void writeRawValue(Object value) {
            throw new UnsupportedOperationException("Array has no raw value");
        }

        final void writeField(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.startField(this.fieldName, this.index);
            WriteCompliantProtoWriteSupport.this.recordConsumer.startGroup();
            List<?> list = (List)value;
            WriteCompliantProtoWriteSupport.this.recordConsumer.startField("list", 0);
            Iterator var3 = list.iterator();

            while(var3.hasNext()) {
                Object listEntry = var3.next();
                WriteCompliantProtoWriteSupport.this.recordConsumer.startGroup();
                WriteCompliantProtoWriteSupport.this.recordConsumer.startField("element", 0);
                this.fieldWriter.writeRawValue(listEntry);
                WriteCompliantProtoWriteSupport.this.recordConsumer.endField("element", 0);
                WriteCompliantProtoWriteSupport.this.recordConsumer.endGroup();
            }

            WriteCompliantProtoWriteSupport.this.recordConsumer.endField("list", 0);
            WriteCompliantProtoWriteSupport.this.recordConsumer.endGroup();
            WriteCompliantProtoWriteSupport.this.recordConsumer.endField(this.fieldName, this.index);
        }
    }

    class MessageWriter extends FieldWriter {
        final FieldWriter[] fieldWriters;

        MessageWriter(Descriptor descriptor, GroupType schema) {
            super();
            List<FieldDescriptor> fields = descriptor.getFields();
            this.fieldWriters = (FieldWriter[])((FieldWriter[])Array.newInstance(FieldWriter.class, fields.size()));

            FieldDescriptor fieldDescriptor;
            Object writer;
            for(Iterator var5 = fields.iterator(); var5.hasNext(); this.fieldWriters[fieldDescriptor.getIndex()] = (FieldWriter)writer) {
                fieldDescriptor = (FieldDescriptor)var5.next();
                String name = fieldDescriptor.getName();
                Type type = schema.getType(name);
                writer = this.createWriter(fieldDescriptor, type);
                if (WriteCompliantProtoWriteSupport.this.writeSpecsCompliant && fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
                    writer = WriteCompliantProtoWriteSupport.this.new ArrayWriter((FieldWriter)writer);
                } else if (!WriteCompliantProtoWriteSupport.this.writeSpecsCompliant && fieldDescriptor.isRepeated()) {
                    writer = WriteCompliantProtoWriteSupport.this.new RepeatedWriter((FieldWriter)writer);
                }

                ((FieldWriter)writer).setFieldName(name);
                ((FieldWriter)writer).setIndex(schema.getFieldIndex(name));
            }

        }

        private FieldWriter createWriter(FieldDescriptor fieldDescriptor, Type type) {
            switch(fieldDescriptor.getJavaType()) {
                case STRING:
                    return WriteCompliantProtoWriteSupport.this.new StringWriter();
                case MESSAGE:
                    return this.createMessageWriter(fieldDescriptor, type);
                case INT:
                    return WriteCompliantProtoWriteSupport.this.new IntWriter();
                case LONG:
                    return WriteCompliantProtoWriteSupport.this.new LongWriter();
                case FLOAT:
                    return WriteCompliantProtoWriteSupport.this.new FloatWriter();
                case DOUBLE:
                    return WriteCompliantProtoWriteSupport.this.new DoubleWriter();
                case ENUM:
                    return WriteCompliantProtoWriteSupport.this.new EnumWriter();
                case BOOLEAN:
                    return WriteCompliantProtoWriteSupport.this.new BooleanWriter();
                case BYTE_STRING:
                    return WriteCompliantProtoWriteSupport.this.new BinaryWriter();
                default:
                    return WriteCompliantProtoWriteSupport.this.unknownType(fieldDescriptor);
            }
        }

        private FieldWriter createMessageWriter(FieldDescriptor fieldDescriptor, Type type) {
            return (FieldWriter)(fieldDescriptor.isMapField() && WriteCompliantProtoWriteSupport.this.writeSpecsCompliant ? this.createMapWriter(fieldDescriptor, type) : WriteCompliantProtoWriteSupport.this.new MessageWriter(fieldDescriptor.getMessageType(), this.getGroupType(type)));
        }

        private GroupType getGroupType(final Type type) {
            LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
            return logicalTypeAnnotation == null ? type.asGroupType() : (GroupType)logicalTypeAnnotation.accept(new LogicalTypeAnnotationVisitor<GroupType>() {
                public Optional<GroupType> visit(ListLogicalTypeAnnotation listLogicalType) {
                    return Optional.ofNullable(type.asGroupType().getType("list").asGroupType().getType("element").asGroupType());
                }

                public Optional<GroupType> visit(MapLogicalTypeAnnotation mapLogicalType) {
                    return Optional.ofNullable(type.asGroupType().getType("key_value").asGroupType().getType("value").asGroupType());
                }
            }).orElse(type.asGroupType());
        }

        private WriteCompliantProtoWriteSupport<T>.MapWriter createMapWriter(FieldDescriptor fieldDescriptor, Type type) {
            List<FieldDescriptor> fields = fieldDescriptor.getMessageType().getFields();
            if (fields.size() != 2) {
                throw new UnsupportedOperationException("Expected two fields for the map (key/value), but got: " + fields);
            } else {
                FieldDescriptor keyProtoField = (FieldDescriptor)fields.get(0);
                FieldWriter keyWriter = this.createWriter(keyProtoField, type);
                keyWriter.setFieldName(keyProtoField.getName());
                keyWriter.setIndex(0);
                FieldDescriptor valueProtoField = (FieldDescriptor)fields.get(1);
                FieldWriter valueWriter = this.createWriter(valueProtoField, type);
                valueWriter.setFieldName(valueProtoField.getName());
                valueWriter.setIndex(1);
                return WriteCompliantProtoWriteSupport.this.new MapWriter(keyWriter, valueWriter);
            }
        }

        void writeTopLevelMessage(Object value) {
            this.writeAllFields((MessageOrBuilder)value);
        }

        final void writeRawValue(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.startGroup();
            this.writeAllFields((MessageOrBuilder)value);
            WriteCompliantProtoWriteSupport.this.recordConsumer.endGroup();
        }

        final void writeField(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.startField(this.fieldName, this.index);
            this.writeRawValue(value);
            WriteCompliantProtoWriteSupport.this.recordConsumer.endField(this.fieldName, this.index);
        }

        private void writeAllFields(MessageOrBuilder pb) {
            Map<FieldDescriptor, Object> changedPbFields = pb.getAllFields();
            Iterator var3 = changedPbFields.entrySet().iterator();

            while(var3.hasNext()) {
                Entry<FieldDescriptor, Object> entry = (Entry)var3.next();
                FieldDescriptor fieldDescriptor = (FieldDescriptor)entry.getKey();
                if (fieldDescriptor.isExtension()) {
                    throw new UnsupportedOperationException("Cannot convert Protobuf message with extension field(s)");
                }

                int fieldIndex = fieldDescriptor.getIndex();
                this.fieldWriters[fieldIndex].writeField(entry.getValue());
            }

        }
    }

    class FieldWriter {
        String fieldName;
        int index = -1;

        FieldWriter() {
        }

        void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        void setIndex(int index) {
            this.index = index;
        }

        void writeRawValue(Object value) {
        }

        void writeField(Object value) {
            WriteCompliantProtoWriteSupport.this.recordConsumer.startField(this.fieldName, this.index);
            this.writeRawValue(value);
            WriteCompliantProtoWriteSupport.this.recordConsumer.endField(this.fieldName, this.index);
        }
    }
}
