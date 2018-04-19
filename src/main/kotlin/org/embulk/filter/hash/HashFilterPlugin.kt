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
                    setRowValue()
                    builder.addRecord()
                }
            }

            private fun setRowValue() {
                for (inputColumn in inputSchema.columns) {
                    if (reader.isNull(inputColumn)) {
                        builder.setNull(inputColumn)
                        continue
                    }

                    when (inputColumn.type) {
                        Types.STRING -> {
                            setColumnValue(inputColumn, reader::getString, builder::setString)
                        }
                        Types.BOOLEAN -> {
                            setColumnValue(inputColumn, reader::getBoolean, builder::setBoolean)
                        }
                        Types.DOUBLE -> {
                            setColumnValue(inputColumn, reader::getDouble, builder::setDouble)
                        }
                        Types.LONG -> {
                            setColumnValue(inputColumn, reader::getLong, builder::setLong)
                        }
                        Types.TIMESTAMP -> {
                            setColumnValue(inputColumn, reader::getTimestamp, builder::setTimestamp)
                        }
                        Types.JSON -> {
                            setColumnValue(inputColumn, reader::getJson, builder::setJson)
                        }
                        else -> {
                            throw DataException("Unexpected type:" + inputColumn.type)
                        }
                    }
                }
            }

            private fun <T> setColumnValue(
                    inputColumn: Column,
                    getter: (inputColumn: Column) -> T,
                    setter: (inputColumn: Column, value: T) -> Unit
            ) {
                val inputValue = getter(inputColumn)

                hashColumnMap[inputColumn.name]?.let { hashColumn ->
                    // Write hashed value if it's hash column.
                    val outputColumn = outputColumnMap[hashColumn.newName.or(inputColumn.name)]
                    val hashedValue = generateHash(inputValue.toString(), hashColumn.algorithm.get())
                    builder.setString(outputColumn, hashedValue)
                } ?: run {
                    // Write the original data
                    setter(inputColumn, inputValue)
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
