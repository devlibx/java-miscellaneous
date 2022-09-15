package io.github.devlibx.miscellaneous.flink.store;

import io.github.devlibx.easy.flink.utils.v2.config.AerospikeConfig;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.flink.utils.v2.config.DynamoDbConfig;
import io.github.devlibx.easy.flink.utils.v2.config.StateStoreConfig;
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
        if (configuration != null && configuration.getStateStore() != null && configuration.getStateStore().isEnableMultiDb()) {
            multiDbSetupEnabled = true;
        } else {
            multiDbSetupEnabled = false;
        }
    }

    public void ensureProxySetupIsDone() {
        if (!multiDbSetupEnabled) {
            if (genericStateStore == null && configuration.getStateStore() != null) {
                if (Objects.equals(configuration.getStateStore().getType(), StateStoreConfig.DYNAMO)) {
                    genericStateStore = new DynamoDBBackedStateStore(configuration.getStateStore().getDdbConfig(), configuration);
                } else if (Objects.equals(configuration.getStateStore().getType(), StateStoreConfig.IN_MEMORY_DYNAMO)) {
                    genericStateStore = new InMemoryDynamoDBBackedStateStore(configuration);
                } else if (Objects.equals(configuration.getStateStore().getType(), StateStoreConfig.AEROSPIKE)) {
                    genericStateStore = new AerospikeBackedStateStore(configuration.getStateStore().getAerospikeDbConfig(), configuration);
                }
            }
            secondaryGenericStateStore = new NoOpGenericStateStore();
        } else {
            StateStoreConfig stateStoreConfig = configuration.getStateStore();
            if (genericStateStore == null && stateStoreConfig != null) {
                DynamoDbConfig dynamoDbConfig = stateStoreConfig.getDdbConfig();
                AerospikeConfig aerospikeConfig = stateStoreConfig.getAerospikeDbConfig();

                if (dynamoDbConfig != null && dynamoDbConfig.isEnabled() && aerospikeConfig != null && aerospikeConfig.isEnabled()) {
                    if (Objects.equals(dynamoDbConfig.getStoreGroup().getName(), aerospikeConfig.getStoreGroup().getName())) {
                        if (dynamoDbConfig.getStoreGroup().getPriority() == 0) {
                            genericStateStore = new DynamoDBBackedStateStore(dynamoDbConfig, configuration);
                            if (aerospikeConfig.getStoreGroup().getPriority() >= 1) {
                                secondaryGenericStateStore = new AerospikeBackedStateStore(aerospikeConfig, configuration);
                            } else {
                                secondaryGenericStateStore = new NoOpGenericStateStore();
                            }
                        } else {
                            genericStateStore = new AerospikeBackedStateStore(aerospikeConfig, configuration);
                            if (dynamoDbConfig.getStoreGroup().getPriority() >= 1) {
                                secondaryGenericStateStore = new DynamoDBBackedStateStore(dynamoDbConfig, configuration);
                            } else {
                                secondaryGenericStateStore = new NoOpGenericStateStore();
                            }
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
