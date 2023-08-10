package org.embulk.parser.avro.getter;

import java.util.Map;

import org.embulk.parser.avro.TimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;

public class BooleanColumnGetter extends BaseColumnGetter {
  protected Boolean value;

  public BooleanColumnGetter(
      PageBuilder pageBuilder,
      Map<String, TimestampFormatter> timestampFormatters,
      Map<String, TimestampUnit> timestampUnits) {
    super(pageBuilder, timestampFormatters, timestampUnits);
  }

  @Override
  public void setValue(Object value) {
    this.value = (Boolean) value;
  }

  @Override
  public void booleanColumn(Column column) {
    if (value == null) {
      pageBuilder.setNull(column);
    } else {
      pageBuilder.setBoolean(column, value);
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
}
