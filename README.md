
# Java Utils Package

### TimeWindowDataAggregation Usage

This is used to aggregate day, hours, and min data

```java
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
```

---

# Flink Module

To run test cases you need to have following 2:

1. DDB Access
    1. Table named "harish-table" with "pk=id" and "sort-key=sub_key"
    3. Edit the ```src/test/resources/test-store.yaml``` to change table name
2. Aerospike
    1. Again you can edit ```src/test/resources/test-store.yaml```
    2. Or run docker

```shell
# Launch Aerospike - change dir "/Users/harishbohara/Downloads/aerospike_data" to your own
docker run -d  --name aerospike -v /Users/harishbohara/Downloads/aerospike_data:/opt/aerospike/data -p 3000-3002:3000-3002 aerospike:ce-5.7.0.12

# See data in your set after running tests (change the IP in -h arg)
docker run -ti  --name aerospike-tools --rm aerospike/aerospike-tools aql -h 192.168.0.126 --no-config-file
>> select * from test.test_set
```
