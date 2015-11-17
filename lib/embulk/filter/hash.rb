Embulk::JavaPlugin.register_filter(
  "hash", "org.embulk.filter.hash.HashFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
