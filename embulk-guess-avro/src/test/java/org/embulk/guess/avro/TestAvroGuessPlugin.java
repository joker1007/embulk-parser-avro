package org.embulk.guess.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigDiff;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.parser.avro.AvroParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.GuessPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestAvroGuessPlugin {

  private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES;

  static {
    final Properties properties = new Properties();
    properties.setProperty("default_guess_plugins", "avro");
    EMBULK_SYSTEM_PROPERTIES = EmbulkSystemProperties.of(properties);
  }

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "avro", AvroParserPlugin.class)
          .registerPlugin(GuessPlugin.class, "avro", AvroGuessPlugin.class)
          .build();

  @Before
  public void setUp() {
    embulk.reset();
  }

  @Test
  public void testAvroFile() throws URISyntaxException {
    Map<String, String> expectedColumns = new HashMap<>();
    expectedColumns.put("id", "long");
    expectedColumns.put("code", "long");
    expectedColumns.put("name", "string");
    expectedColumns.put("description", "string");
    expectedColumns.put("flag", "boolean");
    expectedColumns.put("created_at", "string");
    expectedColumns.put("created_at_utc", "double");
    expectedColumns.put("price", "double");
    expectedColumns.put("spec", "json");
    expectedColumns.put("tags", "json");
    expectedColumns.put("options", "json");
    expectedColumns.put("item_type", "string");
    expectedColumns.put("dummy", "string");

    ConfigDiff configDiff = guess("items.avro");

    assertEquals("avro", configDiff.get(String.class, "type"));

    List<Map> columns = configDiff.getListOf(Map.class, "columns");
    columns.forEach(
        columnConfig -> {
          String name = (String) columnConfig.get("name");
          assertTrue(expectedColumns.containsKey(name));
          assertEquals(expectedColumns.get(name), (String) columnConfig.get("type"));
        });
  }

  @Test
  public void testNonAvroFile() throws URISyntaxException {
    ConfigDiff configDiff = guess("data.json");
    assertFalse(configDiff.has("columns"));
  }

  private ConfigDiff guess(String resource) throws URISyntaxException {
    Path path =
        Paths.get(this.getClass().getResource("/org/embulk/parser/avro/" + resource).toURI());
    return embulk
        .parserBuilder()
        .parser(embulk.newConfig().set("type", "avro"))
        .inputPath(path)
        .guess();
  }
}
