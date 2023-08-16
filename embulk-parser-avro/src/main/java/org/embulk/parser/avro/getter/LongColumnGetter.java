package org.embulk.parser.avro.getter;

import java.util.Map;
import org.embulk.parser.avro.TimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;

public class LongColumnGetter extends BaseColumnGetter {
  protected Long value;

  public LongColumnGetter(
      PageBuilder pageBuilder,
      Map<String, TimestampFormatter> timestampFormatters,
      Map<String, TimestampUnit> timestampUnits) {
    super(pageBuilder, timestampFormatters, timestampUnits);
  }

  @Override
  public void setValue(Object value) {
    this.value = (Long) value;
  }

  @Override
  public void longColumn(Column column) {
    if (value == null) {
      pageBuilder.setNull(column);
    } else {
      pageBuilder.setLong(column, value);
    }
  }

  @Override
  public void doubleColumn(Column column) {
    if (value == null) {
      pageBuilder.setNull(column);
    } else {
      pageBuilder.setDouble(column, value.doubleValue());
    }
  }

  @Override
  public void stringColumn(Column column) {
    if (value == null) {
      pageBuilder.setNull(column);
    } else {
      pageBuilder.setString(column, value.toString());
    }
  }

  @Override
  public void timestampColumn(Column column) {
    if (this.value == null) {
      pageBuilder.setNull(column);
    } else {
      TimestampUnit unit = timestampUnits.get(column.getName());
      // for v0.9.x
      pageBuilder.setTimestamp(column, unit.toTimestamp(value));
    }
  }
}
