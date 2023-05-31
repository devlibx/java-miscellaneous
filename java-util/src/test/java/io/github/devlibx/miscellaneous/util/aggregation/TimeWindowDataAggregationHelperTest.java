package io.github.devlibx.miscellaneous.util.aggregation;


import io.gitbub.devlibx.easy.helper.calendar.CalendarUtils;
import io.gitbub.devlibx.easy.helper.json.JsonUtil;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregationHelper.Config;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregationHelper.IAggregationUpdater;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TimeWindowDataAggregationHelperTest {
    private final DateTime timeToUse = CalendarUtils.createTime(CalendarUtils.DATETIME_FORMAT_V1, "2022-08-08T17:26:35.302+05:30");
    private final DateTime timeToUseInHourTest = CalendarUtils.createTime(CalendarUtils.DATETIME_FORMAT_V1, "2022-09-08T17:26:35.302+05:30");
    private final DateTime timeToUseInMinuteTest = CalendarUtils.createTime(CalendarUtils.DATETIME_FORMAT_V1, "2022-09-08T17:26:35.302+05:30");

    @Test
    public void testGetKeys() {
        TimeWindowDataAggregationHelper<String> helper = new TimeWindowDataAggregationHelper<>(
                Config.builder().dayAggregationWindow(31).hourAggregationWindow(24).minuteAggregationWindow(60).build()
        );
        List<String> days = helper.getDayKeys(timeToUse);
        Assertions.assertEquals(31, days.size());
        Assertions.assertEquals("7-9", days.get(0));
        Assertions.assertEquals("8-8", days.get(30));

        List<String> hours = helper.getHoursKeys(timeToUse);
        Assertions.assertEquals(24, hours.size());
        Assertions.assertEquals("7-18", hours.get(0));
        Assertions.assertEquals("8-17", hours.get(23));

        List<String> minutes = helper.getMinuetKeys(timeToUse);
        Assertions.assertEquals(60, minutes.size());
        Assertions.assertEquals("16-27", minutes.get(0));
        Assertions.assertEquals("17-26", minutes.get(59));
    }

    @Test
    public void testProcessDay() {
        TimeWindowDataAggregation aggregation = new TimeWindowDataAggregation();
        TimeWindowDataAggregationHelper<String> helper = new TimeWindowDataAggregationHelper<>(
                Config.builder().dayAggregationWindow(31).hourAggregationWindow(24).minuteAggregationWindow(60).build()
        );

        // Process aggregation updates
        IAggregationUpdater<String> updater = (data, key, event) -> data.put(key, "processed-" + event);

        // Process data for 30 days back
        helper.processDay(aggregation, timeToUse, "a", timeToUse.minusDays(30), updater);
        Assertions.assertEquals(1, aggregation.getDays().size());
        Assertions.assertEquals("processed-a", aggregation.getDays().get("7-9"));


        // Process data for 31 days back (No new keys are added to aggregation - only old data exists)
        helper.processDay(aggregation, timeToUse, "b", timeToUse.minusDays(31), updater);
        Assertions.assertEquals(1, aggregation.getDays().size());
        Assertions.assertEquals("processed-a", aggregation.getDays().get("7-9"));

        // Process data for today
        helper.processDay(aggregation, timeToUse, "c", timeToUse.minusDays(0), updater);
        Assertions.assertEquals(2, aggregation.getDays().size());
        Assertions.assertEquals("processed-a", aggregation.getDays().get("7-9"));
        Assertions.assertEquals("processed-c", aggregation.getDays().get("8-8"));

        // Process data for today + 1 (No new keys are added to aggregation - time is not good)
        helper.processDay(aggregation, timeToUse, "c", timeToUse.plusDays(1), updater);
        Assertions.assertEquals(2, aggregation.getDays().size());
        Assertions.assertEquals("processed-a", aggregation.getDays().get("7-9"));
        Assertions.assertEquals("processed-c", aggregation.getDays().get("8-8"));

        // Process data for today + 60 (No new keys are added to aggregation - time is not good)
        helper.processDay(aggregation, timeToUse.plusDays(60), "c", timeToUse, updater);
        Assertions.assertEquals(0, aggregation.getDays().size());
    }


    @Test
    public void testProcessHours() {
        TimeWindowDataAggregation aggregation = new TimeWindowDataAggregation();
        TimeWindowDataAggregationHelper<String> helper = new TimeWindowDataAggregationHelper<>(
                Config.builder().dayAggregationWindow(31).hourAggregationWindow(24).minuteAggregationWindow(60).build()
        );

        // Process aggregation updates
        IAggregationUpdater<String> updater = (data, key, event) -> data.put(key, "processed-" + event);

        // Process data for 24 days back
        helper.processHours(aggregation, timeToUseInHourTest, "a", timeToUseInHourTest.minusHours(23), updater);
        Assertions.assertEquals(1, aggregation.getHours().size());
        Assertions.assertEquals("processed-a", aggregation.getHours().get("7-18"));


        // Process data for 31 days back (No new keys are added to aggregation - only old data exists)
        helper.processHours(aggregation, timeToUseInHourTest, "b", timeToUseInHourTest.minusHours(24), updater);
        Assertions.assertEquals(1, aggregation.getHours().size());
        Assertions.assertEquals("processed-a", aggregation.getHours().get("7-18"));

        // Process data for today
        helper.processHours(aggregation, timeToUseInHourTest, "c", timeToUseInHourTest.minusHours(0), updater);
        Assertions.assertEquals(2, aggregation.getHours().size());
        Assertions.assertEquals("processed-a", aggregation.getHours().get("7-18"));
        Assertions.assertEquals("processed-c", aggregation.getHours().get("8-17"));

        // Process data for today + 1 (No new keys are added to aggregation - time is not good)
        helper.processHours(aggregation, timeToUseInHourTest, "c", timeToUseInHourTest.plusHours(1), updater);
        Assertions.assertEquals(2, aggregation.getHours().size());
        Assertions.assertEquals("processed-a", aggregation.getHours().get("7-18"));
        Assertions.assertEquals("processed-c", aggregation.getHours().get("8-17"));

        // Process data for today + 25 (No new keys are added to aggregation - time is not good)
        helper.processHours(aggregation, timeToUseInHourTest.plusHours(25), "c", timeToUseInHourTest, updater);
        Assertions.assertEquals(0, aggregation.getHours().size());
    }

    @Test
    public void testProcessMinutes() {
        TimeWindowDataAggregation aggregation = new TimeWindowDataAggregation();
        TimeWindowDataAggregationHelper<String> helper = new TimeWindowDataAggregationHelper<>(
                Config.builder().dayAggregationWindow(31).hourAggregationWindow(24).minuteAggregationWindow(60).build()
        );

        // Process aggregation updates
        IAggregationUpdater<String> updater = (data, key, event) -> data.put(key, "processed-" + event);

        // Process data for 24 days back
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "a", timeToUseInMinuteTest.minusMinutes(59), updater);
        Assertions.assertEquals(1, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-a", aggregation.getMinutes().get("16-27"));


        // Process data for 31 days back (No new keys are added to aggregation - only old data exists)
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "b", timeToUseInMinuteTest.minusMinutes(60), updater);
        Assertions.assertEquals(1, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-a", aggregation.getMinutes().get("16-27"));

        // Process data for today
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "c", timeToUseInMinuteTest.minusMinutes(0), updater);
        Assertions.assertEquals(2, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-a", aggregation.getMinutes().get("16-27"));
        Assertions.assertEquals("processed-c", aggregation.getMinutes().get("17-26"));

        // Process data for today + 1 (No new keys are added to aggregation - time is not good)
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "c", timeToUseInMinuteTest.plusMinutes(1), updater);
        Assertions.assertEquals(2, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-a", aggregation.getMinutes().get("16-27"));
        Assertions.assertEquals("processed-c", aggregation.getMinutes().get("17-26"));

        // Process data for today + 61 (No new keys are added to aggregation - time is not good)
        helper.processMinutes(aggregation, timeToUseInMinuteTest.plusMinutes(61), "c", timeToUseInMinuteTest, updater);
        Assertions.assertEquals(0, aggregation.getMinutes().size());
    }

    @Test
    public void testProcessDayHours() {
        TimeWindowDataAggregation aggregation = new TimeWindowDataAggregation();
        TimeWindowDataAggregationHelper<String> helper = new TimeWindowDataAggregationHelper<>(
                Config.builder().dayAggregationWindow(31).dayHourAggregationWindow(31).hourAggregationWindow(24).minuteAggregationWindow(60).build()
        );

        // Process aggregation updates
        IAggregationUpdater<String> updater = (data, key, event) -> data.put(key, "processed-" + event);

        // Process data for 30 days back
        helper.processDayHour(aggregation, timeToUse, "a", timeToUse.minusDays(30), updater);
        Assertions.assertEquals(1, aggregation.getDaysHours().size());
        Assertions.assertEquals("processed-a", aggregation.getDaysHours().getStringObjectMap("7-9").get("17"));


        // Process data for 31 days back (No new keys are added to aggregation - only old data exists)
        helper.processDayHour(aggregation, timeToUse, "b", timeToUse.minusDays(31), updater);
        Assertions.assertEquals(1, aggregation.getDaysHours().size());
        Assertions.assertEquals("processed-a", aggregation.getDaysHours().getStringObjectMap("7-9").get("17"));

        // Process data for today
        helper.processDayHour(aggregation, timeToUse, "c", timeToUse.minusDays(0), updater);
        Assertions.assertEquals(2, aggregation.getDaysHours().size());
        Assertions.assertEquals("processed-a", aggregation.getDaysHours().getStringObjectMap("7-9").get("17"));
        Assertions.assertEquals("processed-c", aggregation.getDaysHours().getStringObjectMap("8-8").get("17"));

        // Process data for today + 1 (No new keys are added to aggregation - time is not good)
        helper.processDayHour(aggregation, timeToUse, "c", timeToUse.plusDays(1), updater);
        Assertions.assertEquals(2, aggregation.getDaysHours().size());
        Assertions.assertEquals("processed-a", aggregation.getDaysHours().getStringObjectMap("7-9").get("17"));
        Assertions.assertEquals("processed-c", aggregation.getDaysHours().getStringObjectMap("8-8").get("17"));

        System.out.println(JsonUtils.asJson(aggregation));
    }

}
