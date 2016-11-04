package org.embulk.parser.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
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
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.FileInputInputStream;
import org.embulk.spi.util.Timestamps;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class AvroParserPlugin
        implements ParserPlugin
{
    public interface PluginTask
            extends Task, TimestampParser.Task
    {
        @Config("columns")
        @ConfigDefault("[]")
        public SchemaConfig getColumns();

        @Config("avsc")
        LocalFile getAvsc();
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        File avsc = task.getAvsc().getFile();
        org.apache.avro.Schema avroSchema;
        try {
            avroSchema = new org.apache.avro.Schema.Parser().parse(avsc);
        } catch (IOException e) {
            throw new ConfigException("avsc file is not found");
        }

        Schema schema = buildSchema(task.getColumns(), avroSchema);

        control.run(task.dump(), schema);
    }

    Schema buildSchema(SchemaConfig columns, org.apache.avro.Schema avroSchema) {
        if (columns.size() > 0) {
            return columns.toSchema();
        } else {
            int index = 0;
            ImmutableList.Builder<Column> builder = ImmutableList.builder();
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
                switch (avroType) {
                    case STRING:
                    case BYTES:
                    case FIXED:
                    case ENUM:
                    case NULL:
                        builder.add(new Column(index, name, Types.STRING));
                        index++;
                        break;
                    case INT:
                    case LONG:
                        builder.add(new Column(index, name, Types.LONG));
                        index++;
                        break;
                    case FLOAT:
                    case DOUBLE:
                        builder.add(new Column(index, name, Types.DOUBLE));
                        index++;
                        break;
                    case BOOLEAN:
                        builder.add(new Column(index, name, Types.BOOLEAN));
                        index++;
                        break;
                    case MAP:
                    case ARRAY:
                    case RECORD:
                        builder.add(new Column(index, name, Types.JSON));
                        index++;
                        break;
                    default:
                        throw new RuntimeException("Unsupported type");
                }
            }
            return new Schema(builder.build());
        }
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        List<Column> columns = schema.getColumns();
        final TimestampParser[] timestampParsers = Timestamps.newTimestampColumnParsers(task, task.getColumns());
        File avsc = task.getAvsc().getFile();
        final org.apache.avro.Schema avroSchema;
        try {
            avroSchema = new org.apache.avro.Schema.Parser().parse(avsc);
        } catch (IOException e) {
            throw new ConfigException("avsc file is not found");
        }

        try (FileInputInputStream is = new FileInputInputStream(input); final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            ColumnGetterFactory factory = new ColumnGetterFactory(avroSchema, pageBuilder, timestampParsers);
            ImmutableMap.Builder<String, BaseColumnGetter> columnGettersBuilder = ImmutableMap.builder();
            for (Column column : columns) {
                BaseColumnGetter columnGetter = factory.newColumnGetter(column);
                columnGettersBuilder.put(column.getName(), columnGetter);
            }
            ImmutableMap<String, BaseColumnGetter> columnGetters = columnGettersBuilder.build();
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
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
