package io.github.devlibx.miscellaneous.flink.drools;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.rule.drools.ResultMap;

import java.io.Serializable;

public class DroolBasedKeyFinder implements KafkaSourceHelper.ObjectToKeyConvertor<StringObjectMap>, Serializable {
    private final IRuleEngineProvider ruleEngineProvider;
    private final Configuration configuration;

    public DroolBasedKeyFinder(IRuleEngineProvider ruleEngineProvider) {
        this(ruleEngineProvider, null);
    }

    public DroolBasedKeyFinder(IRuleEngineProvider ruleEngineProvider, Configuration configuration) {
        this.ruleEngineProvider = ruleEngineProvider;
        this.configuration = configuration;
    }

    private String keyAsString(StringObjectMap value) {
        ResultMap result = new ResultMap();
        if (configuration != null) {
            ruleEngineProvider.getDroolsHelper().execute("expiry-event-trigger-partition-key", value, result, configuration);
        } else {
            ruleEngineProvider.getDroolsHelper().execute("expiry-event-trigger-partition-key", value, result);
        }
        return result.getString("partition-key", "");
    }

    @Override
    public byte[] key(StringObjectMap value) {
        return keyAsString(value).getBytes();
    }

    @Override
    public byte[] getKey(StringObjectMap value) {
        return keyAsString(value).getBytes();
    }

    @Override
    public int partition(StringObjectMap value, byte[] bytes, byte[] bytes1, String s, int[] partitions) {
        String key = keyAsString(value);
        return Math.abs(key.hashCode() % partitions.length);
    }
}
