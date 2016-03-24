package org.embulk.filter.hash;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashFilterPlugin implements FilterPlugin {

    public interface PluginTask extends Task {
        @Config("columns")
        List<HashColumn> getColumns();
    }

    public interface HashColumn extends Task {
        @Config("name")
        String getName();

        @Config("algorithm")
        @ConfigDefault("\"SHA-256\"")
        Optional<String> getAlgorithm();

        @Config("new_name")
        @ConfigDefault("null")
        Optional<String> getNewName();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {

        PluginTask task = config.loadConfig(PluginTask.class);
        Map<String, HashColumn> hashColumnMap = convertHashColumnListToMap(task.getColumns());

        Schema.Builder builder = Schema.builder();
        for (Column column : inputSchema.getColumns()) {

            HashColumn hashColumn = hashColumnMap.get(column.getName());

            if (hashColumn != null) {
                builder.add(hashColumn.getNewName().or(column.getName()), Types.STRING);
            } else {
                builder.add(column.getName(), column.getType());
            }
        }
        control.run(task.dump(), builder.build());
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
                           final Schema outputSchema, final PageOutput output) {

        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final Map<String, HashColumn> hashColumnMap = convertHashColumnListToMap(task.getColumns());
        final Map<String, Column> outputColumnMap = convertColumnListToMap(outputSchema.getColumns());

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void add(Page page) {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    setValue();
                    builder.addRecord();
                }
            }

            private void setValue() {
                for (Column inputColumn : inputSchema.getColumns()) {
                    if (reader.isNull(inputColumn)) {
                        builder.setNull(inputColumn);
                        continue;
                    }

                    // Write the original data
                    Object inputValue;
                    if (Types.STRING.equals(inputColumn.getType())) {
                        final String value = reader.getString(inputColumn);
                        inputValue = value;
                        builder.setString(inputColumn, value);
                    } else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                        final boolean value = reader.getBoolean(inputColumn);
                        inputValue = value;
                        builder.setBoolean(inputColumn, value);
                    } else if (Types.DOUBLE.equals(inputColumn.getType())) {
                        final double value = reader.getDouble(inputColumn);
                        inputValue = value;
                        builder.setDouble(inputColumn, value);
                    } else if (Types.LONG.equals(inputColumn.getType())) {
                        final long value = reader.getLong(inputColumn);
                        inputValue = value;
                        builder.setLong(inputColumn, value);
                    } else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                        final Timestamp value = reader.getTimestamp(inputColumn);
                        inputValue = value;
                        builder.setTimestamp(inputColumn, value);
                    } else {
                        throw new DataException("Unexpected type:" + inputColumn.getType());
                    }

                    // Overwrite the column if it's hash column.
                    HashColumn hashColumn = hashColumnMap.get(inputColumn.getName());
                    if (hashColumn != null) {
                        final Column outputColumn = outputColumnMap.get(hashColumn.getNewName().or(inputColumn.getName()));
                        final String hashedValue = generateHash(inputValue.toString(), hashColumn.getAlgorithm().get());
                        builder.setString(outputColumn, hashedValue);
                    }
                }
            }

            private String generateHash(String value, String algorithm) {
                String result = null;
                try {
                    MessageDigest md = MessageDigest.getInstance(algorithm);
                    md.update(value.getBytes());

                    StringBuilder sb = new StringBuilder();
                    for (byte b : md.digest()) {
                        sb.append(String.format("%02x", b));
                    }
                    result = sb.toString();
                } catch (NoSuchAlgorithmException e) {
                    Throwables.propagate(e);
                }
                return result;
            }

            @Override
            public void finish() {
                builder.finish();
            }

            @Override
            public void close() {
                builder.close();
            }
        };
    }

    private static Map<String, HashColumn> convertHashColumnListToMap(List<HashColumn> hashColumns) {
        Map<String, HashColumn> result = new HashMap<>();
        for (HashColumn hashColumn : hashColumns) {
            result.put(hashColumn.getName(), hashColumn);
        }
        return result;
    }

    private static Map<String, Column> convertColumnListToMap(List<Column> columns) {
        Map<String, Column> result = new HashMap<>();
        for (Column column : columns) {
            result.put(column.getName(), column);
        }
        return result;
    }
}
