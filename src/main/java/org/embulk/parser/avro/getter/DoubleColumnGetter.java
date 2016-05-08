package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;

public class DoubleColumnGetter extends BaseColumnGetter {
    protected Double value;

    public DoubleColumnGetter(PageBuilder pageBuilder, TimestampParser[] timestampParsers) {
        super(pageBuilder, timestampParsers);
    }

    @Override
    public void setValue(Object value)
    {
        this.value = (Double) value;
    }

    @Override
    public void longColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setLong(column, value.longValue());
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setDouble(column, value);
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
        }
        else {
            long milliSec = (long) (value * 1000);
            pageBuilder.setTimestamp(column, Timestamp.ofEpochMilli(milliSec));
        }
    }
}
