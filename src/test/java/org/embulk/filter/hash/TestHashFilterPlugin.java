package org.embulk.filter.hash;

import org.embulk.config.ConfigSource;
import org.embulk.spi.FilterPlugin;
import org.embulk.test.EmbulkPluginTest;
import org.embulk.test.TestingEmbulk;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.embulk.spi.type.Types.STRING;
import static org.embulk.test.TestOutputPlugin.assertRecords;
import static org.embulk.test.TestOutputPlugin.assertSchema;
import static org.embulk.test.Utils.column;
import static org.embulk.test.Utils.record;

public class TestHashFilterPlugin extends EmbulkPluginTest {

    @Override
    protected void setup(TestingEmbulk.Builder builder) {
        builder.registerPlugin(FilterPlugin.class, "hash", HashFilterPlugin.class);
    }

    @Test
    public void specifiedColumnIsHashedAndRenamed() {
        final String inConfigPath = "yaml/input_basic.yml";

        ConfigSource config = newConfig()
                .set("type", "hash")
                .set("columns", Collections.singletonList(
                        newConfig().set("name", "age").set("algorithm", "MD5").set("new_name", "hashed_age")
                        )
                );

        runFilter(config, inConfigPath);

        assertSchema(
                column("username", STRING),
                column("hashed_age", STRING)
        );

        assertRecords(
                record("user1", "98f13708210194c475687be6106a3b84")
        );
    }

    @Test
    public void allColumnTypesAreHashed() {
        final String inConfigPath = "yaml/input_column_types.yml";

        ConfigSource config = newConfig()
                .set("type", "hash")
                .set("columns", Arrays.asList(
                        newConfig().set("name", "username"),
                        newConfig().set("name", "age"),
                        newConfig().set("name", "weight"),
                        newConfig().set("name", "active"),
                        newConfig().set("name", "created_at"),
                        newConfig().set("name", "options")
                        )
                );

        runFilter(config, inConfigPath);

        assertSchema(
                column("username", STRING),
                column("age", STRING),
                column("weight", STRING),
                column("active", STRING),
                column("created_at", STRING),
                column("options", STRING)
        );

        assertRecords(
                record(
                        "0a041b9462caa4a31bac3567e0b6e6fd9100787db2ab433d96f6d178cabfce90",
                        "6f4b6612125fb3a0daecd2799dfd6c9c299424fd920f9b308110a2c1fbd8f443",
                        "70822ecbef5bee37d162492107a3127fc0a4de0564f34ce92713a7baaeb582b0",
                        "b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b",
                        "9673fe7b67d880e2c9071428c63f6e1bea9dde98283297277a20b92ea0acdc72",
                        "3ff0e331ca59a2a1194bac0e36359ed4540a97383e1cdf6eb95c7de9309143fc"
                )
        );
    }
}
