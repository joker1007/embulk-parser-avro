package org.embulk.parser.avro.getter;

import org.apache.avro.Schema;
import org.embulk.parser.avro.TimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;

import java.util.Map;

public class ColumnGetterFactory {
  private org.apache.avro.Schema avroSchema;
  private PageBuilder pageBuilder;
  private Map<String, TimestampFormatter> timestampFormatters;
  private Map<String, TimestampUnit> timestampUnits;

  public ColumnGetterFactory(
      org.apache.avro.Schema avroSchema,
      PageBuilder pageBuilder,
      Map<String, TimestampFormatter> timestampFormatters,
      Map<String, TimestampUnit> timestampUnits) {
    this.avroSchema = avroSchema;
    this.pageBuilder = pageBuilder;
    this.timestampFormatters = timestampFormatters;
    this.timestampUnits = timestampUnits;
  }

  public BaseColumnGetter newColumnGetter(Column column) {
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
      default:
        return getColumnGetterFromTypeName(fieldSchema.getType());
    }
  }

  private BaseColumnGetter getColumnGetterFromTypeName(Schema.Type type) {
    switch (type) {
      case STRING:
      case ENUM:
      case NULL:
        return new StringColumnGetter(pageBuilder, timestampFormatters, timestampUnits);
      case INT:
        return new IntegerColumnGetter(pageBuilder, timestampFormatters, timestampUnits);
      case LONG:
        return new LongColumnGetter(pageBuilder, timestampFormatters, timestampUnits);
      case FLOAT:
        return new FloatColumnGetter(pageBuilder, timestampFormatters, timestampUnits);
      case DOUBLE:
        return new DoubleColumnGetter(pageBuilder, timestampFormatters, timestampUnits);
      case BOOLEAN:
        return new BooleanColumnGetter(pageBuilder, timestampFormatters, timestampUnits);
      case ARRAY:
      case MAP:
      case RECORD:
        return new GenericDataColumnGetter(pageBuilder, timestampFormatters, timestampUnits);
      case BYTES:
      default:
        throw new DataException(String.format("%s is not supported", type.getName()));
    }
  }
}
