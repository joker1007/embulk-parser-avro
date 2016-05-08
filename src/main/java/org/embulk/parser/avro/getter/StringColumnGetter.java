package org.embulk.parser.avro.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampParser;

public class StringColumnGetter extends BaseColumnGetter {
    protected String value;
    private final JsonParser jsonParser = new JsonParser();

    public StringColumnGetter(PageBuilder pageBuilder, TimestampParser[] timestampParsers) {
        super(pageBuilder, timestampParsers);
    }

    @Override
    public void setValue(Object value)
    {
        if (value == null)
            this.value = null;
        else
            this.value = value.toString();
    }

    @Override
    public void booleanColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setBoolean(column, Boolean.parseBoolean(value));
        }
    }

    @Override
    public void longColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setLong(column, Long.parseLong(value));
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setDouble(column, Double.parseDouble(value));
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setString(column, value);
        }
    }

    @Override
    public void timestampColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            TimestampParser parser = timestampParsers[column.getIndex()];
            pageBuilder.setTimestamp(column, parser.parse(value));
        }
    }

    @Override
    public void jsonColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setJson(column, jsonParser.parse(value));
        }
    }
}
