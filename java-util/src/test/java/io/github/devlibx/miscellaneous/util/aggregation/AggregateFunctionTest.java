package io.github.devlibx.miscellaneous.util.aggregation;

import io.gitbub.devlibx.easy.helper.calendar.CalendarUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregationHelper.IAggregationUpdater;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AggregateFunctionTest {
    private final DateTime timeToUse = CalendarUtils.createTime(CalendarUtils.DATETIME_FORMAT_V1, "2022-08-08T17:26:35.302+05:30");

    @Test
    public void testDefaultCountAggregateFunction() {
        TimeWindowDataAggregation aggregation = new TimeWindowDataAggregation();
        TimeWindowDataAggregationHelper<Integer> helper = new TimeWindowDataAggregationHelper<>(31, 24, 60);

        // Process aggregation updates
        IAggregationUpdater<Integer> updater = new IAggregationUpdater<Integer>() {
            @Override
            public void update(StringObjectMap data, String key, Integer event) {
                Long existingValue = data.getLong(key) == null
                        ? 0L
                        : data.getLong(key);
                data.put(key, existingValue + event);
            }
        };

        // Process data for 30 days back
        helper.processDay(aggregation, timeToUse, 4, timeToUse.minusDays(30), updater);
        Assertions.assertEquals(1, aggregation.getDays().size());
        Assertions.assertEquals(4, aggregation.getDays().getLong("7-9"));

        // Process data for 3 hours back
        helper.processHours(aggregation, timeToUse, 3, timeToUse.minusHours(3), updater);
        Assertions.assertEquals(1, aggregation.getHours().size());
        Assertions.assertEquals(3, aggregation.getHours().getLong("8-14"));

        // Process data for 3 hours back
        helper.processHours(aggregation, timeToUse, 1, timeToUse.minusHours(3), updater);
        Assertions.assertEquals(1, aggregation.getHours().size());
        Assertions.assertEquals(4, aggregation.getHours().getLong("8-14"));

        // Process data for 5 min back
        helper.processMinutes(aggregation, timeToUse, 7, timeToUse.minusMinutes(3), updater);
        Assertions.assertEquals(1, aggregation.getMinutes().size());
        Assertions.assertEquals(7, aggregation.getMinutes().getLong("17-23"));

        // Process data for 5 min back
        helper.processMinutes(aggregation, timeToUse, 6, timeToUse.minusMinutes(0), updater);
        Assertions.assertEquals(2, aggregation.getMinutes().size());
        Assertions.assertEquals(7, aggregation.getMinutes().getLong("17-23"));
        Assertions.assertEquals(6, aggregation.getMinutes().getLong("17-26"));

        // Process data for 30 days back
        helper.processDay(aggregation, timeToUse, 2, timeToUse.minusDays(30), updater);
        Assertions.assertEquals(1, aggregation.getDays().size());
        Assertions.assertEquals(6, aggregation.getDays().getLong("7-9"));
    }
}
