package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParser;

public class ColumnGetterFactory {
    private org.apache.avro.Schema avroSchema;
    private PageBuilder pageBuilder;
    private TimestampParser[] timestampParsers;

    public ColumnGetterFactory(org.apache.avro.Schema avroSchema, PageBuilder pageBuilder, TimestampParser[] timestampParsers)
    {
        this.avroSchema = avroSchema;
        this.pageBuilder = pageBuilder;
        this.timestampParsers = timestampParsers;
    }

    public BaseColumnGetter newColumnGetter(Column column)
    {
        org.apache.avro.Schema fieldSchema = avroSchema.getField(column.getName()).schema();
        switch (fieldSchema.getType().getName()) {
            case "union" :
                String typeName = "";
                for (org.apache.avro.Schema type : fieldSchema.getTypes()) {
                    if (!type.getName().equals("null")) {
                        typeName = type.getName();
                        break;
                    }
                }
                return getColumnGetterFromTypeName(typeName);
            default :
                return getColumnGetterFromTypeName(fieldSchema.getType().getName());
        }
    }

    private BaseColumnGetter getColumnGetterFromTypeName(String typeName)
    {
        switch (typeName) {
            case "string":
            case "enum":
                return new StringColumnGetter(pageBuilder, timestampParsers);
            case "int":
                return new IntegerColumnGetter(pageBuilder, timestampParsers);
            case "long":
                return new LongColumnGetter(pageBuilder, timestampParsers);
            case "float":
                return new FloatColumnGetter(pageBuilder, timestampParsers);
            case "double":
                return new DoubleColumnGetter(pageBuilder, timestampParsers);
            case "boolean":
                return new BooleanColumnGetter(pageBuilder, timestampParsers);
            case "array":
                return new ArrayColumnGetter(pageBuilder, timestampParsers);
            case "map":
                return new MapColumnGetter(pageBuilder, timestampParsers);
            case "record":
                return new RecordColumnGetter(pageBuilder, timestampParsers);
            case "byte":
            default:
                throw new DataException(String.format("%s is not supported", typeName));
        }
    }
}
