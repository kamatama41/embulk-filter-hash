package org.embulk.filter.hash

import com.google.common.base.Optional
import com.google.common.base.Throwables
import org.embulk.config.Config
import org.embulk.config.ConfigDefault
import org.embulk.config.ConfigSource
import org.embulk.config.Task
import org.embulk.config.TaskSource
import org.embulk.spi.Column
import org.embulk.spi.DataException
import org.embulk.spi.Exec
import org.embulk.spi.FilterPlugin
import org.embulk.spi.Page
import org.embulk.spi.PageBuilder
import org.embulk.spi.PageOutput
import org.embulk.spi.PageReader
import org.embulk.spi.Schema
import org.embulk.spi.type.Types
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.HashMap

class HashFilterPlugin : FilterPlugin {

    interface PluginTask : Task {
        @Config("columns")
        fun getColumns(): List<HashColumn>
    }

    interface HashColumn : Task {
        @Config("name")
        fun getName(): String

        @Config("algorithm")
        @ConfigDefault("\"SHA-256\"")
        fun getAlgorithm(): Optional<String>

        @Config("new_name")
        @ConfigDefault("null")
        fun getNewName(): Optional<String>
    }

    override fun transaction(config: ConfigSource, inputSchema: Schema, control: FilterPlugin.Control) {

        val task = config.loadConfig(PluginTask::class.java)
        val hashColumnMap = convertHashColumnListToMap(task.getColumns())

        val builder = Schema.builder()
        for (column in inputSchema.columns) {

            val hashColumn = hashColumnMap.get(column.name)

            if (hashColumn != null) {
                builder.add(hashColumn!!.getNewName().or(column.name), Types.STRING)
            } else {
                builder.add(column.name, column.type)
            }
        }
        control.run(task.dump(), builder.build())
    }

    override fun open(taskSource: TaskSource, inputSchema: Schema,
                      outputSchema: Schema, output: PageOutput): PageOutput {

        val task = taskSource.loadTask(PluginTask::class.java)
        val hashColumnMap = convertHashColumnListToMap(task.getColumns())
        val outputColumnMap = convertColumnListToMap(outputSchema.columns)

        return object : PageOutput {
            private val reader = PageReader(inputSchema)
            private val builder = PageBuilder(Exec.getBufferAllocator(), outputSchema, output)

            override fun add(page: Page) {
                reader.setPage(page)
                while (reader.nextRecord()) {
                    setValue()
                    builder.addRecord()
                }
            }

            private fun setValue() {
                for (inputColumn in inputSchema.columns) {
                    if (reader.isNull(inputColumn)) {
                        builder.setNull(inputColumn)
                        continue
                    }

                    // Write the original data
                    val inputValue: Any
                    if (Types.STRING == inputColumn.type) {
                        val value = reader.getString(inputColumn)
                        inputValue = value
                        builder.setString(inputColumn, value)
                    } else if (Types.BOOLEAN == inputColumn.type) {
                        val value = reader.getBoolean(inputColumn)
                        inputValue = value
                        builder.setBoolean(inputColumn, value)
                    } else if (Types.DOUBLE == inputColumn.type) {
                        val value = reader.getDouble(inputColumn)
                        inputValue = value
                        builder.setDouble(inputColumn, value)
                    } else if (Types.LONG == inputColumn.type) {
                        val value = reader.getLong(inputColumn)
                        inputValue = value
                        builder.setLong(inputColumn, value)
                    } else if (Types.TIMESTAMP == inputColumn.type) {
                        val value = reader.getTimestamp(inputColumn)
                        inputValue = value
                        builder.setTimestamp(inputColumn, value)
                    } else if (Types.JSON == inputColumn.type) {
                        val value = reader.getJson(inputColumn)
                        inputValue = value
                        builder.setJson(inputColumn, value)
                    } else {
                        throw DataException("Unexpected type:" + inputColumn.type)
                    }

                    // Overwrite the column if it's hash column.
                    val hashColumn = hashColumnMap[inputColumn.name]
                    if (hashColumn != null) {
                        val outputColumn = outputColumnMap[hashColumn.getNewName().or(inputColumn.name)]
                        val hashedValue = generateHash(inputValue.toString(), hashColumn.getAlgorithm().get())
                        builder.setString(outputColumn, hashedValue)
                    }
                }
            }

            private fun generateHash(value: String, algorithm: String): String {
                var result: String? = null
                try {
                    val md = MessageDigest.getInstance(algorithm)
                    md.update(value.toByteArray())

                    val sb = StringBuilder()
                    for (b in md.digest()) {
                        sb.append(String.format("%02x", b))
                    }
                    result = sb.toString()
                } catch (e: NoSuchAlgorithmException) {
                    Throwables.propagate(e)
                }

                return result!!
            }

            override fun finish() {
                builder.finish()
            }

            override fun close() {
                builder.close()
            }
        }
    }

    private fun convertHashColumnListToMap(hashColumns: List<HashColumn>): Map<String, HashColumn> {
        val result = HashMap<String, HashColumn>()
        for (hashColumn in hashColumns) {
            result.put(hashColumn.getName(), hashColumn)
        }
        return result
    }

    private fun convertColumnListToMap(columns: List<Column>): Map<String, Column> {
        val result = HashMap<String, Column>()
        for (column in columns) {
            result.put(column.name, column)
        }
        return result
    }
}
