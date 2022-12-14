package io.github.devlibx.miscellaneous.flink.drools.generator;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.rule.drools.ResultMap;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;

import java.util.Arrays;
import java.util.List;

public class SampleJavaFileFile {
    public static final String primaryKeyPrefix = "user_case_1_pk#";
    public static final String secondaryKeyPrefix = "user_case_1_sk#";
    public static final String primaryIdKey = "user_id";

    /**
     *  System.out.println(JsonUtils.asJson(event));
     */
    public transient String __commonCodeMarker__ = "";

    /**
     * <pre>
     * rule "Filter Input Stream"
     *   dialect "java"
     *   agenda-group "filter-input-stream"
     * when
     *  event : StringObjectMap()
     *  resultMap: ResultMap()
     *  configuration: Configuration()
     *  then
     * </pre>
     */
    public void onEventReceived(StringObjectMap event, ResultMap resultMap, Configuration configuration) {
        final List<String> validStatusList = Arrays.asList("COMPLETED", "INIT");
        final String debugKey = "debug-drools-print-result-filter-input-stream";
        final StringObjectMap miscellaneousProperties = configuration.getMiscellaneousProperties();

        String status = event.get("data", "order_status", String.class);
        String primaryId = event.get(primaryIdKey, String.class);
        if (validStatusList.contains(status)) {
            resultMap.put("skip", false);
            resultMap.put("group-key", primaryId);
        } else {
            resultMap.put("skip", true);
        }

        if (miscellaneousProperties.containsKey(debugKey) && miscellaneousProperties.get(debugKey, Boolean.class)) {
            System.out.println(JsonUtils.asJson(resultMap));
        }
    }

    /**
     * <pre>
     * rule "Fetch State Keys"
     *   dialect "java"
     *   agenda-group "initial-event-trigger-get-state-to-fetch"
     *   when
     *       event : StringObjectMap()
     *       resultMap: ResultMap()
     *       configuration: Configuration()
     *   then
     * </pre>
     */
    public void onEventProcessing_FetchStateKeys(StringObjectMap event, ResultMap resultMap, Configuration configuration) {
        final String debugKey = "debug-drools-print-result-state-keys-func";
        final StringObjectMap miscellaneousProperties = configuration.getMiscellaneousProperties();

        String primaryId = primaryKeyPrefix + event.get(primaryIdKey, String.class);
        String secondaryId = secondaryKeyPrefix + event.get("data", "category", String.class);
        resultMap.put("states-to-provide", Arrays.asList(new KeyPair(primaryId, "na"), new KeyPair(primaryId, secondaryId)));

        if (miscellaneousProperties.containsKey(debugKey) && miscellaneousProperties.get(debugKey, Boolean.class)) {
            System.out.println(JsonUtils.asJson(resultMap));
        }
    }
}
