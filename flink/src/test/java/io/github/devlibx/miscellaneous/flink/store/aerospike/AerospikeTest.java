package io.github.devlibx.miscellaneous.flink.store.aerospike;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.gitbub.devlibx.easy.helper.yaml.YamlUtils;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.flink.utils.v2.config.StateStoreConfig;
import io.github.devlibx.miscellaneous.flink.store.GenericState;
import io.github.devlibx.miscellaneous.flink.store.IGenericStateStore;
import io.github.devlibx.miscellaneous.flink.store.Key;
import io.github.devlibx.miscellaneous.flink.store.ProxyBackedGenericStateStore;
import lombok.Data;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.UUID;

public class AerospikeTest {

    @Test
    @EnabledOnOs(OS.MAC)
    public void testConfig() {
        TestConfigContainer config = YamlUtils.readYamlFromResourcePath("/test-store.yaml", TestConfigContainer.class);
        Assertions.assertNotNull(config);
        System.out.println(JsonUtils.asJson(config));
    }

    @Test
    @EnabledOnOs(OS.MAC)
    public void testAerospike() {
        TestConfigContainer config = YamlUtils.readYamlFromResourcePath("/test-store.yaml", TestConfigContainer.class);

        IGenericStateStore genericStateStore = new AerospikeBackedStateStore(config.getConfig().getAerospikeDbConfig(), null);

        String userId = UUID.randomUUID().toString();
        Key key = Key.builder().key(userId).subKey("na").build();
        genericStateStore.persist(
                key,
                GenericState.builder()
                        .data(StringObjectMap.of("key_1", "value_1"))
                        .ttl(DateTime.now().plusMinutes(10))
                        .build()
        );

        GenericState resultFromDb = genericStateStore.get(key);
        System.out.println(JsonUtils.asJson(resultFromDb));
        Assertions.assertNotNull(resultFromDb);
        Assertions.assertNotNull(resultFromDb.getData());
        Assertions.assertEquals("value_1", resultFromDb.getData().get("key_1"));
    }

    @Test
    @EnabledOnOs(OS.MAC)
    public void testAerospike_WithDDB_WithONlyDDB() {
        TestConfigContainer config = YamlUtils.readYamlFromResourcePath("/test-store.yaml", TestConfigContainer.class);
        config.getConfig().setEnableMultiDb(false);
        Configuration configuration = new Configuration();
        configuration.setStateStore(config.config);
        IGenericStateStore genericStateStore = new ProxyBackedGenericStateStore(configuration);

        String userId = UUID.randomUUID().toString();
        Key key = Key.builder().key(userId).subKey("na").build();
        genericStateStore.persist(
                key,
                GenericState.builder()
                        .data(StringObjectMap.of("key_1", "value_1"))
                        .ttl(DateTime.now().plusMinutes(10))
                        .build()
        );

        GenericState resultFromDb = genericStateStore.get(key);
        System.out.println(JsonUtils.asJson(resultFromDb));
        Assertions.assertNotNull(resultFromDb);
        Assertions.assertNotNull(resultFromDb.getData());
        Assertions.assertEquals("value_1", resultFromDb.getData().get("key_1"));
    }

    @Test
    @EnabledOnOs(OS.MAC)
    public void testAerospike_WithDDB_And_Aerospike() {
        TestConfigContainer config = YamlUtils.readYamlFromResourcePath("/test-store.yaml", TestConfigContainer.class);
        Configuration configuration = new Configuration();
        configuration.setStateStore(config.config);
        IGenericStateStore genericStateStore = new ProxyBackedGenericStateStore(configuration);

        String userId = UUID.randomUUID().toString();
        Key key = Key.builder().key(userId).subKey("na").build();
        System.out.println("AS Key=" + key.getKey() + "#" + key.getSubKey());
        genericStateStore.persist(
                key,
                GenericState.builder()
                        .data(StringObjectMap.of("key_1", "value_1"))
                        .ttl(DateTime.now().plusMinutes(10))
                        .build()
        );

        GenericState resultFromDb = genericStateStore.get(key);
        System.out.println(JsonUtils.asJson(resultFromDb));
        Assertions.assertNotNull(resultFromDb);
        Assertions.assertNotNull(resultFromDb.getData());
        Assertions.assertEquals("value_1", resultFromDb.getData().get("key_1"));
    }

    @Data
    public static class TestConfigContainer {
        @JsonProperty("state_store")
        private StateStoreConfig config;
    }
}
