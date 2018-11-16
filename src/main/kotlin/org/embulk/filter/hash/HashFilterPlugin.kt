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
import java.security.NoSuchAlgorithmException
import java.util.Locale
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

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

        @get:Config("secret_key")
        @get:ConfigDefault("null")
        val secretKey: Optional<String>

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
                // Check algorithm is valid
                getAlgorithmType(hashColumn.algorithm.get()).validate(hashColumn)
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
                    val hashedValue = generateHash(inputValue.toString(), hashColumn)
                    builder.setString(outputColumn, hashedValue)
                } ?: run {
                    // Write the original data
                    setter(inputColumn, inputValue)
                }
            }

            private fun generateHash(value: String, config: HashColumn): String {
                return getAlgorithmType(config.algorithm.get()).generateHash(value, config)
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

    private fun getAlgorithmType(algorithm: String): AlgorithmType {
        return when {
            MD_ALGORITHMS.contains(algorithm.toUpperCase(Locale.ENGLISH)) -> {
                AlgorithmType.MESSAGE_DIGEST
            }
            MAC_ALGORITHMS.contains(algorithm.toUpperCase(Locale.ENGLISH)) -> {
                AlgorithmType.MAC
            }
            else -> throw NoSuchAlgorithmException(algorithm)
        }
    }

    enum class AlgorithmType {
        MESSAGE_DIGEST {
            override fun validate(config: HashColumn) {}

            override fun generateHash(value: String, config: HashColumn): String {
                val algorithm = config.algorithm.get()
                return MessageDigest.getInstance(algorithm).run {
                    update(value.toByteArray())
                    digest().joinToString("") { "%02x".format(it) }
                }
            }
        },
        MAC {
            override fun validate(config: HashColumn) {
                require(config.secretKey.isPresent) { "Secret key must not be null." }
            }

            override fun generateHash(value: String, config: HashColumn): String {
                val secretKey = config.secretKey.get()
                val algorithm = config.algorithm.get()
                return Mac.getInstance(algorithm).run {
                    init(SecretKeySpec(secretKey.toByteArray(), algorithm))
                    doFinal(value.toByteArray()).joinToString("") { "%02x".format(it) }
                }
            }
        };

        abstract fun validate(config: HashColumn)
        abstract fun generateHash(value: String, config: HashColumn): String
    }

    companion object {
        val MD_ALGORITHMS = java.security.Security.getAlgorithms("MessageDigest") ?: emptySet<String>()
        val MAC_ALGORITHMS = java.security.Security.getAlgorithms("Mac") ?: emptySet<String>()
    }
}
