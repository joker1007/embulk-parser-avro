package org.embulk.parser.avro.getter;

import org.embulk.parser.avro.TimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;

import java.util.Map;

public class BaseColumnGetter implements ColumnVisitor {
  protected final PageBuilder pageBuilder;
  protected final Map<String, TimestampFormatter> timestampFormatters;
  protected final Map<String, TimestampUnit> timestampUnits;
  protected Object value;

  public BaseColumnGetter(
      PageBuilder pageBuilder,
      Map<String, TimestampFormatter> timestampFormatters,
      Map<String, TimestampUnit> timestampUnits) {
    this.pageBuilder = pageBuilder;
    this.timestampFormatters = timestampFormatters;
    this.timestampUnits = timestampUnits;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  @Override
  public void booleanColumn(Column column) {
    if (value == null) {
      pageBuilder.setNull(column);
    } else {
      throw new DataException(String.format("cannot convert value from %s", column.getType()));
    }
  }

  @Override
  public void longColumn(Column column) {
    if (value == null) {
      pageBuilder.setNull(column);
    } else {
      throw new DataException(String.format("cannot convert value from %s", column.getType()));
    }
  }

  @Override
  public void doubleColumn(Column column) {
    if (value == null) {
      pageBuilder.setNull(column);
    } else {
      throw new DataException(String.format("cannot convert value from %s", column.getType()));
    }
  }

  @Override
  public void stringColumn(Column column) {
    if (value == null) {
      pageBuilder.setNull(column);
    } else {
      throw new DataException(String.format("cannot convert value from %s", column.getType()));
    }
  }

  @Override
  public void timestampColumn(Column column) {
    if (value == null) {
      pageBuilder.setNull(column);
    } else {
      throw new DataException(String.format("cannot convert value from %s", column.getType()));
    }
  }

  @Override
  public void jsonColumn(Column column) {
    if (value == null) {
      pageBuilder.setNull(column);
    } else {
      throw new DataException(String.format("cannot convert value from %s", column.getType()));
    }
  }
}
