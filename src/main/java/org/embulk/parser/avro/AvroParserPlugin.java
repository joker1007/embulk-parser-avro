package org.embulk.parser.avro;

import com.google.common.base.Optional;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.FileInputInputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class AvroParserPlugin
        implements ParserPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("columns")
        public SchemaConfig getColumns();

        @Config("avsc")
        LocalFile getAvsc();
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();

        control.run(task.dump(), schema);
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        File avsc = task.getAvsc().getFile();

        try (FileInputInputStream is = new FileInputInputStream(input)) {
            org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avsc);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
            GenericRecord record = null;
            while (is.nextFile()) {
                DataFileStream<GenericRecord> ds = new DataFileStream<>(is, reader);
                while (ds.hasNext()) {
                    record = ds.next(record);
                    System.out.println(record);
                }
            }
        }
        catch(IOException e) {
        }

        // Write your code here :)
        throw new UnsupportedOperationException("AvroParserPlugin.run method is not implemented yet");
    }
}
