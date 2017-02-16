package org.embulk.filter.hash

import com.google.common.base.Optional
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

class HashFilterPlugin : FilterPlugin {
    interface PluginTask : Task {
        @get:Config("columns")
        val columns: List<HashColumn>
    }

    interface HashColumn : Task {
        @get:Config("name")
        val name: String

        @get:Config("algorithm")
        @get:ConfigDefault("\"SHA-256\"")
        val algorithm: Optional<String>

        @get:Config("new_name")
        @get:ConfigDefault("null")
        val newName: Optional<String>
    }

    override fun transaction(config: ConfigSource, inputSchema: Schema, control: FilterPlugin.Control) {
        val task: PluginTask = config.loadConfig()
        val hashColumnMap = convertHashColumnListToMap(task.columns)

        val builder = Schema.builder()
        inputSchema.columns.forEach { column ->
            val hashColumn = hashColumnMap[column.name]
            if (hashColumn != null) {
                builder.add(hashColumn.newName.or(column.name), Types.STRING)
            } else {
                builder.add(column.name, column.type)
            }
        }
        control.run(task.dump(), builder.build())
    }

    override fun open(taskSource: TaskSource, inputSchema: Schema,
                      outputSchema: Schema, output: PageOutput): PageOutput {
        val task: PluginTask = taskSource.loadTask()
        val hashColumnMap = convertHashColumnListToMap(task.columns)
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
                    val inputValue: Any = when (inputColumn.type) {
                        Types.STRING -> {
                            reader.getString(inputColumn).apply { builder.setString(inputColumn, this) }
                        }
                        Types.BOOLEAN -> {
                            reader.getBoolean(inputColumn).apply { builder.setBoolean(inputColumn, this) }
                        }
                        Types.DOUBLE -> {
                            reader.getDouble(inputColumn).apply { builder.setDouble(inputColumn, this) }
                        }
                        Types.LONG -> {
                            reader.getLong(inputColumn).apply { builder.setLong(inputColumn, this) }
                        }
                        Types.TIMESTAMP -> {
                            reader.getTimestamp(inputColumn).apply { builder.setTimestamp(inputColumn, this) }
                        }
                        Types.JSON -> {
                            reader.getJson(inputColumn).apply { builder.setJson(inputColumn, this) }
                        }
                        else -> {
                            throw DataException("Unexpected type:" + inputColumn.type)
                        }
                    }

                    // Overwrite the column if it's hash column.
                    hashColumnMap[inputColumn.name]?.let { hashColumn ->
                        val outputColumn = outputColumnMap[hashColumn.newName.or(inputColumn.name)]
                        val hashedValue = generateHash(inputValue.toString(), hashColumn.algorithm.get())
                        builder.setString(outputColumn, hashedValue)
                    }
                }
            }

            private fun generateHash(value: String, algorithm: String): String {
                val md = MessageDigest.getInstance(algorithm)
                md.update(value.toByteArray())
                return md.digest().joinToString("") { "%02x".format(it) }
            }

            override fun finish() {
                builder.finish()
            }

            override fun close() {
                builder.close()
            }
        }
    }

    private fun convertHashColumnListToMap(hashColumns: List<HashColumn>?): Map<String, HashColumn> {
        return hashColumns!!.associate { Pair(it.name, it) }
    }

    private fun convertColumnListToMap(columns: List<Column>?): Map<String, Column> {
        return columns!!.associate { Pair(it.name, it) }
    }
}
