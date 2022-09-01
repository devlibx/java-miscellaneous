package io.github.devlibx.miscellaneous.util.aggregation;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregationHelper.IAggregationUpdater;
import lombok.Builder;
import lombok.Data;
import org.joda.time.DateTime;

public class Demo {

    public static void main(String[] args) {
        TimeWindowDataAggregation aggregation = new TimeWindowDataAggregation();
        TimeWindowDataAggregationHelper<InputObject> helper = new TimeWindowDataAggregationHelper<>(
                TimeWindowDataAggregationHelper.Config.builder().dayHourAggregationWindow(31).hourAggregationWindow(24).minuteAggregationWindow(60).build()
        );

        // Process aggregation updates
        IAggregationUpdater<InputObject> updater = (data, key, event) -> {
            long existingValue = data.getLong(key) == null
                    ? 0L
                    : data.getLong(key);
            data.put(key, existingValue + 1);
        };

        // Process event 1
        DateTime now = DateTime.now();
        InputObject inputObject = InputObject.builder().orderId("1").timestamp(now).build();
        helper.process(aggregation, now, inputObject, inputObject.timestamp, updater);
        System.out.println(JsonUtils.asJson(aggregation));
        // >> OUTPUT = {"updated_at":1662020452255,"days_hours":{"9-1":{"13":1}},"hours":{"1-13":1},"minutes":{"13-50":1}}

        // Process event 2
        now = DateTime.now();
        inputObject = InputObject.builder().orderId("1").timestamp(now).build();
        helper.process(aggregation, now, inputObject, inputObject.timestamp, updater);
        System.out.println(JsonUtils.asJson(aggregation));
        // >> OUTPUT = {"updated_at":1662020452476,"days_hours":{"9-1":{"13":2}},"hours":{"1-13":2},"minutes":{"13-50":2}}

        // Process event 3
        now = DateTime.now();
        inputObject = InputObject.builder().orderId("1").timestamp(now).build();
        helper.process(aggregation, now, inputObject, inputObject.timestamp, updater);
        System.out.println(JsonUtils.asJson(aggregation));
        // >> OUTPUT = {"updated_at":1662020452476,"days_hours":{"9-1":{"13":3}},"hours":{"1-13":3},"minutes":{"13-50":3}}
    }

    @Data
    @Builder
    public static class InputObject {
        private String orderId;
        DateTime timestamp;
    }
}
