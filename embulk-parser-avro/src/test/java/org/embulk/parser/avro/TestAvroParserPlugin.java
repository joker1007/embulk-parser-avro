package org.embulk.parser.avro;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.FileInput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.type.Type;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.Pages;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.test.TestPageBuilderReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueFactory;

public class TestAvroParserPlugin {
  @Rule public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

  private ConfigSource config;
  private AvroParserPlugin plugin;
  private TestPageBuilderReader.MockPageOutput output;

  @Before
  public void createResource() {
    config = config().set("type", "avro");
    plugin = new AvroParserPlugin();

    recreatePageOutput();
  }

  @Test
  public void useNormal() throws Exception {
    SchemaConfig schema =
        schema(
            column("id", LONG),
            column("code", STRING),
            column("name", STRING),
            column("description", STRING),
            column("flag", BOOLEAN),
            column("price", DOUBLE),
            column("item_type", STRING),
            column("tags", JSON),
            column("options", JSON),
            column("spec", JSON),
            column("created_at", TIMESTAMP, config().set("format", "%Y-%m-%dT%H:%M:%S%:z")),
            column("created_at_utc", TIMESTAMP, config().set("timestamp_unit", "second")));

    ConfigSource config =
        this.config
            .deepCopy()
            .set("columns", schema)
            .set("avsc", this.getClass().getResource("item.avsc").getPath());

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
    assertEquals("bar", ((MapValue) record[8]).map().get(ValueFactory.newString("foo")).toString());
    assertEquals(
        "opt1", ((MapValue) record[9]).map().get(ValueFactory.newString("key")).toString());
    assertEquals("2016-05-08 19:35:43 UTC", record[10].toString());
    assertEquals("2016-05-08 19:35:28 UTC", record[11].toString());
  }

  @Test
  public void useTimestampUnit() throws Exception {
    SchemaConfig schema =
        schema(
            column("timestamp", TIMESTAMP, config().set("timestamp_unit", "second")),
            column("timestamp_long", TIMESTAMP, config().set("timestamp_unit", "second")),
            column("timestamp_milli", TIMESTAMP, config().set("timestamp_unit", "milli")),
            column("timestamp_micro", TIMESTAMP, config().set("timestamp_unit", "micro")),
            column("timestamp_nano", TIMESTAMP, config().set("timestamp_unit", "nano")),
            column("timestamp_float", TIMESTAMP, config().set("timestamp_unit", "second")),
            column("timestamp_double", TIMESTAMP, config().set("timestamp_unit", "second")),
            column("timestamp_double_milli", TIMESTAMP, config().set("timestamp_unit", "milli")),
            column("timestamp_double_micro", TIMESTAMP, config().set("timestamp_unit", "micro")),
            column("timestamp_double_nano", TIMESTAMP, config().set("timestamp_unit", "nano")));

    ConfigSource config =
        this.config
            .deepCopy()
            .set("columns", schema)
            .set("avsc", this.getClass().getResource("item2.avsc").getPath());

    transaction(config, fileInput(new File(this.getClass().getResource("items2.avro").getPath())));

    List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
    assertEquals(1, records.size());

    Object[] record = records.get(0);
    assertEquals("2018-02-23 12:13:52 UTC", record[0].toString());
    assertEquals("2018-02-23 12:13:52 UTC", record[1].toString());
    assertEquals("2018-02-23 12:13:52.717 UTC", record[2].toString());
    assertEquals("2018-02-23 12:13:52.717249 UTC", record[3].toString());
    assertEquals("2018-02-23 12:13:52.717249634 UTC", record[4].toString());
    assertEquals("2018-02-23 12:13:52 UTC", record[5].toString());
    assertTrue(record[6].toString().matches("2018-02-23 12:13:52.717249.* UTC"));
    assertTrue(record[7].toString().matches("2018-02-23 12:13:52.717249.* UTC"));
    assertTrue(record[8].toString().matches("2018-02-23 12:13:52.717249.* UTC"));
    assertTrue(record[9].toString().matches("2018-02-23 12:13:52.717249.* UTC"));
  }

  @Test
  public void useNoColumnsOption() throws Exception {
    SchemaConfig schema =
        schema(
            column("id", LONG),
            column("code", LONG),
            column("name", STRING),
            column("description", STRING),
            column("flag", BOOLEAN),
            column("created_at", STRING),
            column("created_at_utc", DOUBLE),
            column("price", DOUBLE),
            column("spec", JSON),
            column("tags", JSON),
            column("options", JSON),
            column("item_type", STRING),
            column("dummy", STRING));

    ConfigSource config =
        this.config.deepCopy().set("avsc", this.getClass().getResource("item.avsc").getPath());

    transaction(config, fileInput(new File(this.getClass().getResource("items.avro").getPath())));

    List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
    assertEquals(6, records.size());

    Object[] record = records.get(0);
    assertEquals(1L, record[0]);
    assertEquals(123456789012345678L, record[1]);
    assertEquals("Desktop", record[2]);
    assertEquals(true, record[4]);
    assertEquals("D", record[11]);
    assertEquals("[\"tag1\",\"tag2\"]", record[9].toString());
    assertEquals(
        "bar", ((MapValue) record[10]).map().get(ValueFactory.newString("foo")).toString());
    assertEquals(
        "opt1", ((MapValue) record[8]).map().get(ValueFactory.newString("key")).toString());
    assertEquals("2016-05-09T04:35:43+09:00", record[5].toString());
    assertNull(record[12]);
  }

  private void recreatePageOutput() {
    output = new TestPageBuilderReader.MockPageOutput();
  }

  private ConfigSource config() {
    return runtime.getExec().newConfigSource();
  }

  private void transaction(ConfigSource config, final FileInput input) {
    plugin.transaction(
        config, (taskSource, schema) -> plugin.run(taskSource, schema, input, output));
  }

  private FileInput fileInput(File file) throws Exception {
    FileInputStream in = new FileInputStream(file);
    return new InputStreamFileInput(runtime.getBufferAllocator(), provider(in));
  }

  private InputStreamFileInput.IteratorProvider provider(InputStream... inputStreams)
      throws IOException {
    return new InputStreamFileInput.IteratorProvider(Arrays.asList(inputStreams));
  }

  private SchemaConfig schema(ColumnConfig... columns) {
    return new SchemaConfig(Arrays.asList(columns));
  }

  private ColumnConfig column(String name, Type type) {
    return column(name, type, config());
  }

  private ColumnConfig column(String name, Type type, ConfigSource option) {
    return new ColumnConfig(name, type, option);
  }
}
