package io.github.devlibx.miscellaneous.flink.store;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

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
        genericStateStore.persist(
                keyPair.buildKey(),
                GenericState.builder().data(
                        JsonUtils.convertAsStringObjectMap(JsonUtils.asJson(value.get("aggregation")))
                ).build()
        );
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        genericStateStore.finish();
    }
}
