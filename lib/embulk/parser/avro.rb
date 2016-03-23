Embulk::JavaPlugin.register_parser(
  "avro", "org.embulk.parser.avro.AvroParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
