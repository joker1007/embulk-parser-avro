package org.embulk.parser.avro.getter;

import java.util.Map;
import org.embulk.parser.avro.TimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

public class GenericDataColumnGetter extends BaseColumnGetter {

  public GenericDataColumnGetter(
      PageBuilder pageBuilder,
      Map<String, TimestampFormatter> timestampFormatters,
      Map<String, TimestampUnit> timestampUnits) {
    super(pageBuilder, timestampFormatters, timestampUnits);
  }

  @Override
  public void stringColumn(Column column) {
    if (this.value == null) {
      pageBuilder.setNull(column);
    } else {
      Value converted = AvroGenericDataConverter.convert(value);
      pageBuilder.setString(column, converted.toString());
    }
  }

  @Override
  public void jsonColumn(Column column) {
    if (this.value == null) {
      pageBuilder.setNull(column);
    } else {
      Value converted = AvroGenericDataConverter.convert(value);
      // for v0.9.x
      pageBuilder.setJson(column, converted);
    }
  }
}
