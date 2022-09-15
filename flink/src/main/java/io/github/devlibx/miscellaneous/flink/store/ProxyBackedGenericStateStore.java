package io.github.devlibx.miscellaneous.flink.store;

import io.github.devlibx.easy.flink.utils.v2.config.AerospikeConfig;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.flink.utils.v2.config.DynamoDbConfig;
import io.github.devlibx.miscellaneous.flink.store.aerospike.AerospikeBackedStateStore;
import io.github.devlibx.miscellaneous.flink.store.ddb.DynamoDBBackedStateStore;
import io.github.devlibx.miscellaneous.flink.store.ddb.InMemoryDynamoDBBackedStateStore;

import java.io.Serializable;
import java.util.Objects;

@SuppressWarnings("RedundantIfStatement")
public class ProxyBackedGenericStateStore implements IGenericStateStore, Serializable {
    private IGenericStateStore genericStateStore;
    private IGenericStateStore secondaryGenericStateStore;
    private final Configuration configuration;
    private final boolean multiDbSetupEnabled;

    public ProxyBackedGenericStateStore(Configuration configuration) {
        this.configuration = configuration;
        if (configuration.getStateStore() != null && !configuration.getStateStore().isEnableMultiDb()) {
            multiDbSetupEnabled = true;
        } else {
            multiDbSetupEnabled = false;
        }
    }

    public void ensureProxySetupIsDone() {
        if (!multiDbSetupEnabled) {
            if (genericStateStore == null && configuration.getStateStore() != null) {
                if (Objects.equals(configuration.getStateStore().getType(), "dynamo")) {
                    genericStateStore = new DynamoDBBackedStateStore(configuration.getStateStore().getDdbConfig(), configuration);
                } else if (Objects.equals(configuration.getStateStore().getType(), "dynamo-in-memory")) {
                    genericStateStore = new InMemoryDynamoDBBackedStateStore(configuration);
                }
            }
            secondaryGenericStateStore = new NoOpGenericStateStore();
        } else {
            if (genericStateStore == null && configuration.getStateStore() != null) {

                DynamoDbConfig dynamoDbConfig = configuration.getStateStore().getDdbConfig();
                AerospikeConfig aerospikeConfig = configuration.getStateStore().getAerospikeDbConfig();

                if (dynamoDbConfig != null && dynamoDbConfig.isEnabled() && aerospikeConfig != null && aerospikeConfig.isEnabled()) {
                    if (Objects.equals(dynamoDbConfig.getStoreGroup().getName(), aerospikeConfig.getStoreGroup().getName())) {
                        if (dynamoDbConfig.getStoreGroup().getPriority() == 0) {
                            genericStateStore = new DynamoDBBackedStateStore(configuration.getStateStore().getDdbConfig(), configuration);
                            secondaryGenericStateStore = new AerospikeBackedStateStore(configuration.getStateStore().getAerospikeDbConfig(), configuration);
                        } else {
                            genericStateStore = new AerospikeBackedStateStore(configuration.getStateStore().getAerospikeDbConfig(), configuration);
                            secondaryGenericStateStore = new DynamoDBBackedStateStore(configuration.getStateStore().getDdbConfig(), configuration);

                        }
                    }
                }

                if (genericStateStore == null || secondaryGenericStateStore == null) {
                    throw new RuntimeException("Multi DB is setup but config is not correct");
                }
            }
        }
    }

    @Override
    public void persist(Key key, GenericState state) {
        ensureProxySetupIsDone();
        genericStateStore.persist(key, state);
        secondaryGenericStateStore.persist(key, state);
    }

    @Override
    public GenericState get(Key key) {
        ensureProxySetupIsDone();
        GenericState result = genericStateStore.get(key);
        if (result == null) {
            return secondaryGenericStateStore.get(key);
        } else {
            return result;
        }
    }
}
