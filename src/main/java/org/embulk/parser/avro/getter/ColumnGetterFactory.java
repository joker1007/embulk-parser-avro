package org.embulk.parser.avro.getter;

import org.apache.avro.Schema;
import org.embulk.parser.avro.TimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParser;

public class ColumnGetterFactory {
    private org.apache.avro.Schema avroSchema;
    private PageBuilder pageBuilder;
    private TimestampParser[] timestampParsers;
    private TimestampUnit[] timestampUnits;

    public ColumnGetterFactory(org.apache.avro.Schema avroSchema, PageBuilder pageBuilder, TimestampParser[] timestampParsers, TimestampUnit[] timestampUnits)
    {
        this.avroSchema = avroSchema;
        this.pageBuilder = pageBuilder;
        this.timestampParsers = timestampParsers;
        this.timestampUnits = timestampUnits;
    }

    public BaseColumnGetter newColumnGetter(Column column)
    {
        org.apache.avro.Schema fieldSchema = avroSchema.getField(column.getName()).schema();
        switch (fieldSchema.getType()) {
            case UNION:
                Schema.Type type = null;
                for (org.apache.avro.Schema sc : fieldSchema.getTypes()) {
                    if (sc.getType() != Schema.Type.NULL) {
                        type = sc.getType();
                        break;
                    }
                }
                return getColumnGetterFromTypeName(type);
            default :
                return getColumnGetterFromTypeName(fieldSchema.getType());
        }
    }

    private BaseColumnGetter getColumnGetterFromTypeName(Schema.Type type)
    {
        switch (type) {
            case STRING:
            case ENUM:
                return new StringColumnGetter(pageBuilder, timestampParsers);
            case INT:
                return new IntegerColumnGetter(pageBuilder, timestampParsers, timestampUnits);
            case LONG:
                return new LongColumnGetter(pageBuilder, timestampParsers, timestampUnits);
            case FLOAT:
                return new FloatColumnGetter(pageBuilder, timestampParsers, timestampUnits);
            case DOUBLE:
                return new DoubleColumnGetter(pageBuilder, timestampParsers, timestampUnits);
            case BOOLEAN:
                return new BooleanColumnGetter(pageBuilder, timestampParsers);
            case ARRAY:
            case MAP:
            case RECORD:
                return new GenericDataColumnGetter(pageBuilder, timestampParsers);
            case NULL:
                return new StringColumnGetter(pageBuilder, timestampParsers);
            case BYTES:
            default:
                throw new DataException(String.format("%s is not supported", type.getName()));
        }
    }
}
