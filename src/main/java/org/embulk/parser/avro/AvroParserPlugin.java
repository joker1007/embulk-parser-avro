package org.embulk.parser.avro;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.parser.avro.getter.BaseColumnGetter;
import org.embulk.parser.avro.getter.ColumnGetterFactory;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Types;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.LocalFile;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.file.FileInputInputStream;
import org.embulk.util.timestamp.TimestampFormatter;

public class AvroParserPlugin implements ParserPlugin {
  private final ConfigMapperFactory configMapperFactory = ConfigMapperFactory.withDefault();

  public interface PluginTask extends Task {
    @Config("columns")
    @ConfigDefault("[]")
    SchemaConfig getColumns();

    @Config("avsc")
    LocalFile getAvsc();

    // from org.embulk.spi.time.TimestampParser.Task
    @Config("default_timezone")
    @ConfigDefault("\"UTC\"")
    String getDefaultTimeZoneId();

    // from org.embulk.spi.time.TimestampParser.Task
    @Config("default_timestamp_format")
    @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
    String getDefaultTimestampFormat();

    // from org.embulk.spi.time.TimestampParser.Task
    @Config("default_date")
    @ConfigDefault("\"1970-01-01\"")
    String getDefaultDate();

    @Config("default_timestamp_unit")
    @ConfigDefault("\"second\"")
    TimestampUnit getDefaultTimestampUnit();
  }

  public interface TimestampColumnConfig extends Task {
    @Config("timezone")
    @ConfigDefault("null")
    Optional<String> getTimeZoneId();

    @Config("format")
    @ConfigDefault("null")
    Optional<String> getFormat();

    @Config("date")
    @ConfigDefault("null")
    Optional<String> getDate();

    @Config("timestamp_unit")
    @ConfigDefault("null")
    Optional<TimestampUnit> getTimestampUnit();
  }

  @Override
  public void transaction(ConfigSource config, ParserPlugin.Control control) {
    ConfigMapper configMapper = configMapperFactory.createConfigMapper();
    PluginTask task = configMapper.map(config, PluginTask.class);

    File avsc = task.getAvsc().getFile();
    org.apache.avro.Schema avroSchema;
    try {
      avroSchema = new org.apache.avro.Schema.Parser().parse(avsc);
    } catch (IOException e) {
      throw new ConfigException("avsc file is not found");
    }

    Schema schema = buildSchema(task.getColumns(), avroSchema);

    control.run(task.toTaskSource(), schema);
  }

  Schema buildSchema(SchemaConfig columns, org.apache.avro.Schema avroSchema) {
    if (columns.size() > 0) {
      return columns.toSchema();
    } else {
      int index = 0;
      List<Column> columnList = new ArrayList<>();
      for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
        String name = field.name();

        org.apache.avro.Schema.Type avroType = null;
        if (field.schema().getType() == org.apache.avro.Schema.Type.UNION) {
          for (org.apache.avro.Schema sc : field.schema().getTypes()) {
            if (sc.getType() != org.apache.avro.Schema.Type.NULL) {
              avroType = sc.getType();
              break;
            }
          }
        } else {
          avroType = field.schema().getType();
        }

        switch (Objects.requireNonNull(avroType)) {
          case STRING:
          case BYTES:
          case FIXED:
          case ENUM:
          case NULL:
            columnList.add(new Column(index, name, Types.STRING));
            index++;
            break;
          case INT:
          case LONG:
            columnList.add(new Column(index, name, Types.LONG));
            index++;
            break;
          case FLOAT:
          case DOUBLE:
            columnList.add(new Column(index, name, Types.DOUBLE));
            index++;
            break;
          case BOOLEAN:
            columnList.add(new Column(index, name, Types.BOOLEAN));
            index++;
            break;
          case MAP:
          case ARRAY:
          case RECORD:
            columnList.add(new Column(index, name, Types.JSON));
            index++;
            break;
          default:
            throw new ConfigException("Unsupported type");
        }
      }
      return new Schema(columnList);
    }
  }

  @Override
  public void run(TaskSource taskSource, Schema schema, FileInput input, PageOutput output) {
    TaskMapper taskMapper = configMapperFactory.createTaskMapper();
    PluginTask task = taskMapper.map(taskSource, PluginTask.class);
    List<Column> columns = schema.getColumns();
    Map<String, TimestampFormatter> timestampFormatters = new HashMap<>();
    Map<String, TimestampUnit> timestampUnits = new HashMap<>();

    ConfigMapper configMapper = configMapperFactory.createConfigMapper();
    task.getColumns()
        .getColumns()
        .forEach(
            columnConfig -> {
              TimestampColumnConfig timestampColumnConfig =
                  configMapper.map(columnConfig.getOption(), TimestampColumnConfig.class);
              TimestampFormatter formatter =
                  TimestampFormatter.builder(
                          timestampColumnConfig
                              .getFormat()
                              .orElse(task.getDefaultTimestampFormat()),
                          true)
                      .setDefaultZoneFromString(
                          timestampColumnConfig.getTimeZoneId().orElse(task.getDefaultTimeZoneId()))
                      .setDefaultDateFromString(
                          timestampColumnConfig.getDate().orElse(task.getDefaultDate()))
                      .build();
              timestampFormatters.put(columnConfig.getName(), formatter);

              if (columnConfig.getType() instanceof TimestampType) {
                timestampUnits.put(
                    columnConfig.getName(),
                    timestampColumnConfig
                        .getTimestampUnit()
                        .orElse(task.getDefaultTimestampUnit()));
              }
            });

    File avsc = task.getAvsc().getFile();
    final org.apache.avro.Schema avroSchema;
    try {
      avroSchema = new org.apache.avro.Schema.Parser().parse(avsc);
    } catch (IOException e) {
      throw new ConfigException("avsc file is not found");
    }

    try (FileInputInputStream is = new FileInputInputStream(input);
        // for compatibility with v0.9.x
        final PageBuilder pageBuilder =
            new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
      ColumnGetterFactory factory =
          new ColumnGetterFactory(avroSchema, pageBuilder, timestampFormatters, timestampUnits);
      Map<String, BaseColumnGetter> columnGetters = new HashMap<>();
      for (Column column : columns) {
        BaseColumnGetter columnGetter = factory.newColumnGetter(column);
        columnGetters.put(column.getName(), columnGetter);
      }
      DatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
      GenericRecord record = null;
      while (is.nextFile()) {
        DataFileStream<GenericRecord> ds = new DataFileStream<>(is, reader);
        while (ds.hasNext()) {
          record = ds.next(record);
          for (Column column : columns) {
            BaseColumnGetter columnGetter = columnGetters.get(column.getName());
            columnGetter.setValue(record.get(column.getName()));
            column.visit(columnGetter);
          }
          pageBuilder.addRecord();
        }
      }

      pageBuilder.finish();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
