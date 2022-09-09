package io.github.devlibx.miscellaneous.flink.drools;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.rule.drools.ResultMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class DroolsBasedFilterFunction implements FilterFunction<StringObjectMap>, KeySelector<StringObjectMap, String> {
    private final IRuleEngineProvider ruleEngineProvider;
    private final Configuration configuration;

    public DroolsBasedFilterFunction(IRuleEngineProvider ruleEngineProvider) {
        this.ruleEngineProvider = ruleEngineProvider;
        this.configuration = null;
    }

    public DroolsBasedFilterFunction(IRuleEngineProvider ruleEngineProvider, Configuration configuration) {
        this.ruleEngineProvider = ruleEngineProvider;
        this.configuration = configuration;
    }

    @Override
    public boolean filter(StringObjectMap value) throws Exception {

        // Make a new session - we will mark agenda-group to run selected rules
        ResultMap result = new ResultMap();
        if (configuration != null) {
            ruleEngineProvider.getDroolsHelper().execute("filter-input-stream", value, result, configuration);
        } else {
            ruleEngineProvider.getDroolsHelper().execute("filter-input-stream", value, result);
        }

        // Skip if rule engine skips it
        return !result.getBoolean("skip", false);
    }

    @Override
    public String getKey(StringObjectMap value) throws Exception {
        ResultMap result = new ResultMap();
        if (configuration != null) {
            ruleEngineProvider.getDroolsHelper().execute("filter-input-stream", value, result, configuration);
        } else {
            ruleEngineProvider.getDroolsHelper().execute("filter-input-stream", value, result);
        }
        return result.getString("group-key");
    }
}
