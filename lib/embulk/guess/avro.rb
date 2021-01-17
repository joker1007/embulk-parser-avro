Embulk::JavaPlugin.register_guess(
  "avro", "org.embulk.guess.avro.AvroGuessPlugin",
  File.expand_path('../../../../classpath', __FILE__))
