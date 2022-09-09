package io.github.devlibx.miscellaneous.flink.store;

import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.miscellaneous.flink.store.ddb.DynamoDBBackedStateStore;
import io.github.devlibx.miscellaneous.flink.store.ddb.InMemoryDynamoDBBackedStateStore;

import java.io.Serializable;
import java.util.Objects;

public class ProxyBackedGenericStateStore implements IGenericStateStore, Serializable {
    private IGenericStateStore genericStateStore;
    private final Configuration configuration;

    public ProxyBackedGenericStateStore(Configuration configuration) {
        this.configuration = configuration;
    }

    public void ensureProxySetupIsDone() {
        if (genericStateStore == null && configuration.getStateStore() != null) {
            if (Objects.equals(configuration.getStateStore().getType(), "dynamo")) {
                genericStateStore = new DynamoDBBackedStateStore(configuration.getStateStore().getDdbConfig(), configuration);
            } else if (Objects.equals(configuration.getStateStore().getType(), "dynamo-in-memory")) {
                genericStateStore = new InMemoryDynamoDBBackedStateStore(configuration);
            }
        }
    }

    @Override
    public void persist(Key key, GenericState state) {
        ensureProxySetupIsDone();
        genericStateStore.persist(key, state);
    }

    @Override
    public GenericState get(Key key) {
        ensureProxySetupIsDone();
        return genericStateStore.get(key);
    }
}
