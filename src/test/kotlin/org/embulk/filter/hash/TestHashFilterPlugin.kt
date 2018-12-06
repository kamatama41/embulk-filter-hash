package org.embulk.filter.hash

import org.embulk.config.ConfigException
import org.embulk.exec.PartialExecutionException
import org.embulk.test.EmbulkPluginTest

import org.embulk.spi.type.Types.STRING
import org.embulk.test.EmbulkTest
import org.embulk.test.TestOutputPlugin.Matcher.assertRecords
import org.embulk.test.TestOutputPlugin.Matcher.assertSchema
import org.embulk.test.record
import org.embulk.test.set
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.instanceOf
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.jupiter.api.Test

@EmbulkTest(value = [HashFilterPlugin::class])
class TestHashFilterPlugin : EmbulkPluginTest() {
    @Test
    fun specifiedColumnIsHashedAndRenamed() {
        val config = config().set(
                "type" to "hash",
                "columns" to listOf(config().set(
                        "name" to "age",
                        "algorithm" to "MD5",
                        "new_name" to "hashed_age"
                )))

        runFilter(config, inConfigPath = "yaml/input_basic.yml")

        assertSchema(
                "username" to STRING,
                "hashed_age" to STRING
        )

        assertRecords(
                record("user1", "98f13708210194c475687be6106a3b84")
        )
    }

    @Test
    fun allColumnTypesAreHashed() {
        val config = config().set(
                "type" to "hash",
                "columns" to listOf(
                        config().set("name" to "username"),
                        config().set("name" to "age"),
                        config().set("name" to "weight"),
                        config().set("name" to "active"),
                        config().set("name" to "created_at"),
                        config().set("name" to "options")
                ))
        runFilter(config, inConfigPath = "yaml/input_column_types.yml")

        assertSchema(
                "username" to STRING,
                "age" to STRING,
                "weight" to STRING,
                "active" to STRING,
                "created_at" to STRING,
                "options" to STRING
        )

        assertRecords(
                record(
                        "0a041b9462caa4a31bac3567e0b6e6fd9100787db2ab433d96f6d178cabfce90",
                        "6f4b6612125fb3a0daecd2799dfd6c9c299424fd920f9b308110a2c1fbd8f443",
                        "70822ecbef5bee37d162492107a3127fc0a4de0564f34ce92713a7baaeb582b0",
                        "b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b",
                        "9673fe7b67d880e2c9071428c63f6e1bea9dde98283297277a20b92ea0acdc72",
                        "3ff0e331ca59a2a1194bac0e36359ed4540a97383e1cdf6eb95c7de9309143fc"
                )
        )
    }

    @Test
    fun specifiedColumnIsHashedByMac() {
        val config = config().set(
                "type" to "hash",
                "columns" to listOf(config().set(
                        "name" to "age",
                        "algorithm" to "HmacSHA256",
                        "secret_key" to "passw0rd",
                        "new_name" to "hashed_age"
                )))

        runFilter(config, inConfigPath = "yaml/input_basic.yml")

        assertSchema(
                "username" to STRING,
                "hashed_age" to STRING
        )

        assertRecords(
                record("user1", "5f9959eac71ad30782ebf4d3c98d12a4c33eadee156a6c5d3881204030811989")
        )
    }

    @Test
    fun exceptionThrownWithInvalidAlgorithm() {
        try {
            val config = config().set(
                    "type" to "hash",
                    "columns" to listOf(config().set(
                            "name" to "age",
                            "algorithm" to "Foo"
                    )))
            runFilter(config, inConfigPath = "yaml/input_basic.yml")
            fail("No exception")
        } catch (e: PartialExecutionException) {
            assertThat(e.cause, instanceOf(ConfigException::class.java))
            assertThat(e.cause?.message, `is`("No such algorithm: Foo"))
        }
    }

    @Test
    fun exceptionThrownWithMacAndNoSecretKey() {
        try {
            val config = config().set(
                    "type" to "hash",
                    "columns" to listOf(config().set(
                            "name" to "age",
                            "algorithm" to "HmacSHA256"
                    )))
            runFilter(config, inConfigPath = "yaml/input_basic.yml")
            fail("No exception")
        } catch (e: PartialExecutionException) {
            assertThat(e.cause, instanceOf(ConfigException::class.java))
            assertThat(e.cause?.message, `is`("Secret key must not be null."))
        }
    }

    @Test
    fun columnIsNull() {
        val config = config().set(
                "type" to "hash",
                "columns" to listOf(
                        config().set("name" to "username"),
                        config().set("name" to "age")
                ))

        runFilter(config, inConfigPath = "yaml/input_null_column.yml")

        assertRecords(
                record(null, "f5ca38f748a1d6eaf726b8a42fb575c3c71f1864a8143301782de13da2d9202b")
        )
    }
}
