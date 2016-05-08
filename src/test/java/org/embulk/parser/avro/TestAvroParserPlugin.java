package org.embulk.parser.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.FileInput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Type;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.Pages;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.JSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestAvroParserPlugin
{

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;
    private AvroParserPlugin plugin;
    private MockPageOutput output;

    @Before
    public void createResource()
    {
        config = config().set("type", "avro");
        plugin = new AvroParserPlugin();
        recreatePageOutput();
    }

    @Test
    public void useNormal()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("id", LONG),
                column("code", STRING),
                column("name", STRING),
                column("description", STRING),
                column("flag", BOOLEAN),
                column("price", DOUBLE),
                column("item_type", STRING),
                column("tags", JSON),
                column("options", JSON),
                column("spec", JSON)
        );

        ConfigSource config = this.config.deepCopy().set("columns", schema).set("avsc", this.getClass().getResource("item.avsc").getPath());

        transaction(config, fileInput(new File(this.getClass().getResource("items.avro").getPath())));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(6, records.size());

        Object[] record = records.get(0);
        assertEquals(1L, record[0]);
        assertEquals("123456789012345678", record[1]);
        assertEquals("Desktop", record[2]);
        assertEquals(true, record[4]);
        assertEquals("D", record[6]);
        assertEquals("[\"tag1\",\"tag2\"]", record[7].toString());
        assertEquals("bar", ((MapValue)record[8]).map().get(ValueFactory.newString("foo")).toString());
        assertEquals("opt1", ((MapValue)record[9]).map().get(ValueFactory.newString("key")).toString());
    }

    private void recreatePageOutput()
    {
        output = new MockPageOutput();
    }

    private ConfigSource config()
    {
        return runtime.getExec().newConfigSource();
    }

    private void transaction(ConfigSource config, final FileInput input)
    {
        plugin.transaction(config, new ParserPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                plugin.run(taskSource, schema, input, output);
            }
        });
    }

    private FileInput fileInput(File file)
            throws Exception
    {
        FileInputStream in = new FileInputStream(file);
        return new InputStreamFileInput(runtime.getBufferAllocator(), provider(in));
    }

    private InputStreamFileInput.IteratorProvider provider(InputStream... inputStreams)
            throws IOException
    {
        return new InputStreamFileInput.IteratorProvider(
                ImmutableList.copyOf(inputStreams));
    }

    private SchemaConfig schema(ColumnConfig... columns)
    {
        return new SchemaConfig(Lists.newArrayList(columns));
    }

    private ColumnConfig column(String name, Type type)
    {
        return column(name, type, config());
    }

    private ColumnConfig column(String name, Type type, ConfigSource option)
    {
        return new ColumnConfig(name, type, option);
    }
}
