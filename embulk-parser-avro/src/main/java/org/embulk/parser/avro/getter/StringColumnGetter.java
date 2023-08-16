package org.embulk.parser.avro.getter;

import java.util.Map;
import org.embulk.parser.avro.TimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;

public class StringColumnGetter extends BaseColumnGetter {
  protected String value;
  private final JsonParser jsonParser = new JsonParser();

  public StringColumnGetter(
      PageBuilder pageBuilder,
      Map<String, TimestampFormatter> timestampFormatters,
      Map<String, TimestampUnit> timestampUnits) {
    super(pageBuilder, timestampFormatters, timestampUnits);
  }

  @Override
  public void setValue(Object value) {
    if (value == null) this.value = null;
    else this.value = value.toString();
  }

  @Override
  public void booleanColumn(Column column) {
    if (this.value == null) {
      pageBuilder.setNull(column);
    } else {
      pageBuilder.setBoolean(column, Boolean.parseBoolean(value));
    }
  }

  @Override
  public void longColumn(Column column) {
    if (this.value == null) {
      pageBuilder.setNull(column);
    } else {
      pageBuilder.setLong(column, Long.parseLong(value));
    }
  }

  @Override
  public void doubleColumn(Column column) {
    if (this.value == null) {
      pageBuilder.setNull(column);
    } else {
      pageBuilder.setDouble(column, Double.parseDouble(value));
    }
  }

  @Override
  public void stringColumn(Column column) {
    if (this.value == null) {
      pageBuilder.setNull(column);
    } else {
      pageBuilder.setString(column, value);
    }
  }

  @Override
  public void timestampColumn(Column column) {
    if (this.value == null) {
      pageBuilder.setNull(column);
    } else {
      TimestampFormatter timestampFormatter = timestampFormatters.get(column.getName());
      // for v0.9.x
      pageBuilder.setTimestamp(column, Timestamp.ofInstant(timestampFormatter.parse(value)));
    }
  }

  @Override
  public void jsonColumn(Column column) {
    if (this.value == null) {
      pageBuilder.setNull(column);
    } else {
      // for v0.9.x
      pageBuilder.setJson(column, jsonParser.parse(value));
    }
  }
}
