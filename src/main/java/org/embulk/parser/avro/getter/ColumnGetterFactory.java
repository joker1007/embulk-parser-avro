package org.embulk.parser.avro.getter;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.embulk.config.ConfigException;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParser;

import java.util.List;

public class ColumnGetterFactory {
    private PageBuilder pageBuilder;
    private TimestampParser[] timestampParsers;

    public ColumnGetterFactory(PageBuilder pageBuilder, TimestampParser[] timestampParsers)
    {
        this.pageBuilder = pageBuilder;
        this.timestampParsers = timestampParsers;
    }

    public ImmutableMap<String, BaseColumnGetter> buildColumnGetters(Schema avroSchema, List<Column> columns) {

        ImmutableMap.Builder<String, BaseColumnGetter> columnGettersBuilder = ImmutableMap.builder();
        for (Column column : columns) {
            BaseColumnGetter columnGetter = newColumnGetter(avroSchema, column);
            columnGettersBuilder.put(column.getName(), columnGetter);
        }
        return columnGettersBuilder.build();
    }

    private BaseColumnGetter newColumnGetter(Schema avroSchema, Column column)
    {
        Schema.Field field = avroSchema.getField(column.getName());
        if (field == null) {
            throw new ConfigException("Unknown field '" + column.getName() + "'. 'avsc' is required to set its default value.");
        }
        Schema fieldSchema = field.schema();
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
                return new IntegerColumnGetter(pageBuilder, timestampParsers);
            case LONG:
                return new LongColumnGetter(pageBuilder, timestampParsers);
            case FLOAT:
                return new FloatColumnGetter(pageBuilder, timestampParsers);
            case DOUBLE:
                return new DoubleColumnGetter(pageBuilder, timestampParsers);
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
