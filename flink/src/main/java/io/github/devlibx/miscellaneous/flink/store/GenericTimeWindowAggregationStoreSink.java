package io.github.devlibx.miscellaneous.flink.store;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.joda.time.DateTime;

@Slf4j
public class GenericTimeWindowAggregationStoreSink extends RichSinkFunction<StringObjectMap> {
    private final IGenericStateStore genericStateStore;

    public GenericTimeWindowAggregationStoreSink(IGenericStateStore genericStateStore) {
        this.genericStateStore = genericStateStore;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        genericStateStore.open(parameters);
    }

    @Override
    public void invoke(StringObjectMap value, Context context) throws Exception {
        super.invoke(value, context);
        KeyPair keyPair = value.get("key_pair", KeyPair.class);

        // Add the aggregation data
        GenericState.GenericStateBuilder state = GenericState.builder()
                .data(JsonUtils.convertAsStringObjectMap(JsonUtils.asJson(value.get("aggregation"))));

        // Set the TTL value if provided
        if (value.containsKey("ttl")) {
            if (value.get("ttl") instanceof Number) {
                state.ttl(value.getDateTimeFromMiles("ttl"));
            } else if (value.get("ttl") instanceof DateTime) {
                state.ttl(value.get("ttl", DateTime.class));
            }
        }

        // Send to store to persist
        genericStateStore.persist(keyPair.buildKey(), state.build());
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        genericStateStore.finish();
    }
}
