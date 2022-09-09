package io.github.devlibx.miscellaneous.flink.store.ddb;

import com.google.common.base.Strings;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.miscellaneous.flink.store.GenericState;
import io.github.devlibx.miscellaneous.flink.store.IGenericStateStore;
import io.github.devlibx.miscellaneous.flink.store.Key;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class InMemoryDynamoDBBackedStateStore implements IGenericStateStore, Serializable {
    private final Map<String, Map<String, GenericState>> data = new HashMap<>();
    private final boolean printLogs;

    public InMemoryDynamoDBBackedStateStore(Configuration configuration) {
        printLogs = configuration.getMiscellaneousProperties().containsKey("debug-dynamo-in-memory-operations")
                && configuration.getMiscellaneousProperties().get("debug-dynamo-in-memory-operations", Boolean.class);
    }

    @Override
    public void persist(Key key, GenericState state) {
        if (!data.containsKey(key.getKey())) {
            data.put(key.getKey(), new HashMap<>());
        }
        if (Strings.isNullOrEmpty(key.getSubKey())) {
            data.get(key.getKey()).put("__internal_sort_key__", state);
            if (printLogs) {
                log.info("DDB-In-Memory Persist: (without sort key): key={}, data={}", key.getKey(), state);
            }
        } else {
            data.get(key.getKey()).put(key.getSubKey(), state);
            if (printLogs) {
                log.info("DDB-In-Memory Persist: key={}, subKey={}, data={}", key.getKey(), key.getSubKey(), state);
            }
        }
    }

    @Override
    public GenericState get(Key key) {
        if (!data.containsKey(key.getKey())) {
            data.put(key.getKey(), new HashMap<>());
        }
        if (Strings.isNullOrEmpty(key.getSubKey())) {
            GenericState result = data.get(key.getKey()).get("__internal_sort_key__");
            if (printLogs) {
                log.info("DDB-In-Memory Get: key={}, data={}", key.getKey(), result);
            }
            return result;
        } else {
            GenericState result = data.get(key.getKey()).get(key.getSubKey());
            if (printLogs) {
                log.info("DDB-In-Memory Get: key={}, subKey={}, data={}", key.getKey(), key.getSubKey(), result);
            }
            return result;
        }
    }
}
