package org.embulk.guess.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.utils.IOUtils;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Buffer;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAvroGuessPlugin {
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().build();

    private AvroGuessPlugin plugin;
    private ConfigSource config;

    @Before
    public void setUp() {
        plugin = new AvroGuessPlugin();
        config = CONFIG_MAPPER_FACTORY.newConfigSource();
    }

    @Test
    public void testAvroFile() throws IOException {
        Map<String, String> expectedColumns = ImmutableMap.<String, String>builder()
                .put("id", "long")
                .put("code", "long")
                .put("name", "string")
                .put("description", "string")
                .put("flag", "boolean")
                .put("created_at", "string")
                .put("created_at_utc", "double")
                .put("price", "double")
                .put("spec", "json")
                .put("tags", "json")
                .put("options", "json")
                .put("item_type", "string")
                .put("dummy", "string")
                .build();

        ConfigDiff configDiff = guess("items.avro");

        JsonNode parserNode = configDiff.getObjectNode().get("parser");
        assertEquals("avro", parserNode.get("type").asText());

        Iterator<JsonNode> it = parserNode.get("columns").elements();
        while (it.hasNext()) {
            JsonNode node = it.next();
            String name = node.get("name").asText();
            assertTrue(expectedColumns.containsKey(name));
            assertEquals(expectedColumns.get(name), node.get("type").asText());
        }
    }

    @Test
    public void testNonAvroFile() throws IOException {
        ConfigDiff configDiff = guess("data.json");
        ObjectNode objectNode = configDiff.getObjectNode();
        assertEquals(0, objectNode.size());
    }

    private ConfigDiff guess(String resource) throws IOException {
        InputStream is = this.getClass().getResourceAsStream("/org/embulk/parser/avro/" + resource);
        Buffer sample = Buffer.wrap(IOUtils.toByteArray(is));
        return plugin.guess(config, sample);
    }
}
