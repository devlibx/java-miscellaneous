package io.github.devlibx.miscellaneous.flink.store.ddb;


import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.flink.utils.v2.config.DynamoDbConfig;
import io.github.devlibx.easy.flink.utils.v2.config.StateStoreConfig;
import io.github.devlibx.miscellaneous.flink.store.GenericState;
import io.github.devlibx.miscellaneous.flink.store.IGenericStateStore;
import io.github.devlibx.miscellaneous.flink.store.Key;
import io.github.devlibx.miscellaneous.flink.store.ProxyBackedGenericStateStore;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.UUID;

public class DynamoDBBackedStateStoreTest {

    @Test
    @EnabledOnOs(OS.MAC)
    public void testDdbStore() {
        String id = "automation-" + DateTime.now() + "-" + UUID.randomUUID();
        StateStoreConfig stateStoreConfig = new StateStoreConfig();
        stateStoreConfig.setType("dynamo");
        stateStoreConfig.setDdbConfig(DynamoDbConfig.builder().table("harish-table").region("ap-south-1").build());
        Configuration configuration = new Configuration();
        configuration.setStateStore(stateStoreConfig);

        IGenericStateStore dynamoDBBackedStateStore = new ProxyBackedGenericStateStore(configuration);

        dynamoDBBackedStateStore.persist(
                Key.builder()
                        .key(id)
                        .subKey("*")
                        .build(),
                GenericState.builder()
                        .data(StringObjectMap.of("name", "harish"))
                        .build()
        );

        GenericState fromDb = dynamoDBBackedStateStore.get(Key.builder().key(id).subKey("*").build());
        Assertions.assertNotNull(fromDb);
        Assertions.assertEquals(StringObjectMap.of("name", "harish"), fromDb.getData());
        System.out.println("--> ID in DDB = " + id);
    }

    @Test
    public void testInMemoryDdbStore() {
        String id = UUID.randomUUID().toString();
        StateStoreConfig stateStoreConfig = new StateStoreConfig();
        stateStoreConfig.setType("dynamo-in-memory");
        stateStoreConfig.setDdbMustHaveSortKey(true);
        Configuration configuration = new Configuration();
        configuration.setStateStore(stateStoreConfig);
        configuration.getMiscellaneousProperties().put(
                "debug-dynamo-in-memory-operations", true
        );

        IGenericStateStore dynamoDBBackedStateStore = new ProxyBackedGenericStateStore(configuration);

        dynamoDBBackedStateStore.persist(
                Key.builder()
                        .key(id)
                        .subKey("*")
                        .build(),
                GenericState.builder()
                        .data(StringObjectMap.of("name", "harish"))
                        .build()
        );

        GenericState fromDb = dynamoDBBackedStateStore.get(Key.builder().key(id).subKey("*").build());
        Assertions.assertNotNull(fromDb);
        Assertions.assertEquals(StringObjectMap.of("name", "harish"), fromDb.getData());


        dynamoDBBackedStateStore.persist(
                Key.builder()
                        .key(id)
                        .build(),
                GenericState.builder()
                        .data(StringObjectMap.of("name", "harish_1"))
                        .build()
        );

        fromDb = dynamoDBBackedStateStore.get(Key.builder().key(id).build());
        Assertions.assertNotNull(fromDb);
        Assertions.assertEquals(StringObjectMap.of("name", "harish_1"), fromDb.getData());
    }
}