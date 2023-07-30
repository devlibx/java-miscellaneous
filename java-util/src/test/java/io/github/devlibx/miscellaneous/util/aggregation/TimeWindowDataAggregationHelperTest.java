package io.github.devlibx.miscellaneous.util.aggregation;


import io.gitbub.devlibx.easy.helper.calendar.CalendarUtils;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregationHelper.Config;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregationHelper.IAggregationUpdater;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TimeWindowDataAggregationHelperTest {
    private DateTime timeToUse = CalendarUtils.createTime(CalendarUtils.DATETIME_FORMAT_V1, "2022-08-08T17:26:35.302+05:30");
    private DateTime timeToUseInHourTest = CalendarUtils.createTime(CalendarUtils.DATETIME_FORMAT_V1, "2022-09-08T17:26:35.302+05:30");
    private DateTime timeToUseInMinuteTest = CalendarUtils.createTime(CalendarUtils.DATETIME_FORMAT_V1, "2022-09-08T17:26:35.302+05:30");

    @Test
    public void testGetKeys() {
        TimeWindowDataAggregationHelper<String> helper = new TimeWindowDataAggregationHelper<>(
                Config.builder().dayAggregationWindow(31).hourAggregationWindow(24).minuteAggregationWindow(120).build()
        );
        List<String> days = helper.getDayKeys(timeToUse);
        Assertions.assertEquals(31, days.size());
        Assertions.assertEquals("7-9", days.get(0));
        Assertions.assertEquals("8-8", days.get(30));

        List<String> hours = helper.getHoursKeys(timeToUse);
        Assertions.assertEquals(24, hours.size());
        Assertions.assertEquals("7-18", hours.get(0));
        Assertions.assertEquals("8-17", hours.get(23));

        List<String> minutes = helper.getMinuteKeys(timeToUse);
        Assertions.assertEquals(120, minutes.size());
        Assertions.assertEquals("15-27", minutes.get(0));
        Assertions.assertEquals("16-26", minutes.get(59));
        Assertions.assertEquals("17-26", minutes.get(119));
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

        // Process data for 31 days back (No new keys are added to aggregation - time is not good, only old data exists)
        helper.processDay(aggregation, timeToUse, "b", timeToUse.minusDays(31), updater);
        Assertions.assertEquals(1, aggregation.getDays().size());
        Assertions.assertEquals("processed-a", aggregation.getDays().get("7-9"));

        // Process data for same time
        helper.processDay(aggregation, timeToUse, "c", timeToUse.minusDays(0), updater);
        Assertions.assertEquals(2, aggregation.getDays().size());
        Assertions.assertEquals("processed-a", aggregation.getDays().get("7-9"));
        Assertions.assertEquals("processed-c", aggregation.getDays().get("8-8"));

        // Process data for 1 day ahead (No new keys are added to aggregation - time is not good, only old keys exist)
        helper.processDay(aggregation, timeToUse, "c", timeToUse.plusDays(1), updater);
        Assertions.assertEquals(2, aggregation.getDays().size());
        Assertions.assertEquals("processed-a", aggregation.getDays().get("7-9"));
        Assertions.assertEquals("processed-c", aggregation.getDays().get("8-8"));

        // Process data for time - 60 days (No new keys are added to aggregation - time is not good, old keys are removed)
        helper.processDay(aggregation, timeToUse.plusDays(60), "c", timeToUse, updater);
        Assertions.assertEquals(0, aggregation.getDays().size());

        timeToUse = timeToUse.plusDays(105);

        // Process data for 16 days back with a gap of 105 days between last updated_at (New key added to aggregation, old keys are removed)
        helper.processDay(aggregation, timeToUse, "c", timeToUse.minusDays(16), updater);
        Assertions.assertEquals(1, aggregation.getDays().size());
        Assertions.assertEquals("processed-c", aggregation.getDays().get("11-5"));

        timeToUse = timeToUse.plusDays(13);

        // Process data for 375 days back which is little more than 1 year and a gap of 13 days between last updated_at (No new keys are added to aggregation - time is not good)
        helper.processDay(aggregation, timeToUse, "b", timeToUse.minusDays(375), updater);
        Assertions.assertEquals(1, aggregation.getDays().size());
        Assertions.assertEquals("processed-c", aggregation.getDays().get("11-5"));

        // Process data for currentTime (New key added to aggregation, old keys are removed)
        helper.processDay(aggregation, DateTime.now().plusDays(35), "a", DateTime.now().plusDays(30), updater);
        Assertions.assertEquals(1, aggregation.getDays().size());
    }

    @Test
    public void testProcessHours() {
        TimeWindowDataAggregation aggregation = new TimeWindowDataAggregation();
        TimeWindowDataAggregationHelper<String> helper = new TimeWindowDataAggregationHelper<>(
                Config.builder().dayAggregationWindow(31).hourAggregationWindow(24).minuteAggregationWindow(60).build()
        );

        // Process aggregation updates
        IAggregationUpdater<String> updater = (data, key, event) -> data.put(key, "processed-" + event);

        // Process data for 23 hours back (New keys added to aggregation)
        helper.processHours(aggregation, timeToUseInHourTest, "a", timeToUseInHourTest.minusHours(23), updater);
        Assertions.assertEquals(1, aggregation.getHours().size());
        Assertions.assertEquals("processed-a", aggregation.getHours().get("7-18"));

        // Process data for 24 hours back (No new keys are added to aggregation - time is not good, old data exists)
        helper.processHours(aggregation, timeToUseInHourTest, "b", timeToUseInHourTest.minusHours(24), updater);
        Assertions.assertEquals(1, aggregation.getHours().size());
        Assertions.assertEquals("processed-a", aggregation.getHours().get("7-18"));

        // Process data for same time (New key added to aggregation)
        helper.processHours(aggregation, timeToUseInHourTest, "c", timeToUseInHourTest.minusHours(0), updater);
        Assertions.assertEquals(2, aggregation.getHours().size());
        Assertions.assertEquals("processed-a", aggregation.getHours().get("7-18"));
        Assertions.assertEquals("processed-c", aggregation.getHours().get("8-17"));

        // Process data for 1 hour ahead (No new keys are added to aggregation - time is not good, old data exists)
        helper.processHours(aggregation, timeToUseInHourTest, "c", timeToUseInHourTest.plusHours(1), updater);
        Assertions.assertEquals(2, aggregation.getHours().size());
        Assertions.assertEquals("processed-a", aggregation.getHours().get("7-18"));
        Assertions.assertEquals("processed-c", aggregation.getHours().get("8-17"));

        // Process data for time - 25 hours (No new keys are added to aggregation - time is not good, old keys are removed)
        helper.processHours(aggregation, timeToUseInHourTest.plusHours(25), "c", timeToUseInHourTest, updater);
        Assertions.assertEquals(0, aggregation.getHours().size());

        timeToUseInHourTest = timeToUseInHourTest.plusHours(55);

        // Process data for 20 hours back with a gap of 55 hours between last updated_at (New key added to aggregation, old keys are removed)
        helper.processHours(aggregation, timeToUseInHourTest, "d", timeToUseInHourTest.minusHours(20), updater);
        Assertions.assertEquals(1, aggregation.getHours().size());
        Assertions.assertEquals("processed-d", aggregation.getHours().get("10-4"));

        timeToUseInHourTest = timeToUseInHourTest.plusHours(17);

        // Process data for 722 hours back which is equal to a little more than 1 month and a gap of 17 hours between last updated_at (No new keys are added to aggregation - time is not good, old keys are removed)
        helper.processHours(aggregation, timeToUseInHourTest, "d", timeToUseInHourTest.minusHours(722), updater);
        Assertions.assertEquals(0, aggregation.getHours().size());

        timeToUseInHourTest = timeToUseInHourTest.plusHours(4);

        // Process data for 16 hours back with a gap of 4 hours between last updated_at (New key added to aggregation)
        helper.processHours(aggregation, timeToUseInHourTest, "d", timeToUseInHourTest.minusHours(16), updater);
        Assertions.assertEquals(1, aggregation.getHours().size());
        Assertions.assertEquals("processed-d", aggregation.getHours().get("11-5"));

        // Process data for currentTime (New key added to aggregation, old keys are removed)
        helper.processHours(aggregation, DateTime.now().plusHours(35), "a", DateTime.now().plusHours(30), updater);
        Assertions.assertEquals(1, aggregation.getHours().size());
    }

    @Test
    public void testProcessMinutes() {
        TimeWindowDataAggregation aggregation = new TimeWindowDataAggregation();
        TimeWindowDataAggregationHelper<String> helper = new TimeWindowDataAggregationHelper<>(
                Config.builder().dayAggregationWindow(31).hourAggregationWindow(24).minuteAggregationWindow(60).build()
        );

        // Process aggregation updates
        IAggregationUpdater<String> updater = (data, key, event) -> data.put(key, "processed-" + event);

        // Process data for 59 minutes back (New key added to aggregation)
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "a", timeToUseInMinuteTest.minusMinutes(59), updater);
        Assertions.assertEquals(1, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-a", aggregation.getMinutes().get("16-27"));

        // Process data for 60 minutes back (No new keys are added to aggregation - only old data exists)
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "b", timeToUseInMinuteTest.minusMinutes(60), updater);
        Assertions.assertEquals(1, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-a", aggregation.getMinutes().get("16-27"));

        // Process data for same time (New key added to aggregation)
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "c", timeToUseInMinuteTest.minusMinutes(0), updater);
        Assertions.assertEquals(2, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-a", aggregation.getMinutes().get("16-27"));
        Assertions.assertEquals("processed-c", aggregation.getMinutes().get("17-26"));

        // Process data for 1 minute ahead (No new keys are added to aggregation - time is not good)
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "c", timeToUseInMinuteTest.plusMinutes(1), updater);
        Assertions.assertEquals(2, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-a", aggregation.getMinutes().get("16-27"));
        Assertions.assertEquals("processed-c", aggregation.getMinutes().get("17-26"));

        // Process data for time - 61 minutes (No new keys are added to aggregation - time is not good, old keys are removed)
        helper.processMinutes(aggregation, timeToUseInMinuteTest.plusMinutes(61), "c", timeToUseInMinuteTest, updater);
        Assertions.assertEquals(0, aggregation.getMinutes().size());

        timeToUseInMinuteTest = timeToUseInMinuteTest.plusMinutes(125);

        // Process data form 15 minutes back with a gap of 125 minutes between last updated_at (No new keys added to aggregation - time is not good, old keys are removed)
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "c", timeToUseInMinuteTest.minusMinutes(15), updater);
        Assertions.assertEquals(1, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-c", aggregation.getMinutes().get("19-16"));

        timeToUseInMinuteTest = timeToUseInMinuteTest.plusMinutes(76);

        // Process data for 61 minutes back with a gap of 76 minutes between last updated_at (New key added to aggregation - old keys are removed)
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "c", timeToUseInMinuteTest.minusMinutes(16), updater);
        Assertions.assertEquals(1, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-c", aggregation.getMinutes().get("20-31"));

        timeToUseInMinuteTest = timeToUseInMinuteTest.plusMinutes(15);

        // Process data for 1445 minutes back which is a little more than 1 day (No new keys are added to aggregation - time is not good)
        helper.processMinutes(aggregation, timeToUseInMinuteTest, "c", timeToUseInMinuteTest.minusMinutes(1445), updater);
        Assertions.assertEquals(1, aggregation.getMinutes().size());
        Assertions.assertEquals("processed-c", aggregation.getMinutes().get("20-31"));

        // Process data for currentTime (New key added to aggregation, old keys are removed)
        helper.processMinutes(aggregation, DateTime.now().plusMinutes(86), "a", DateTime.now().plusMinutes(80), updater);
        Assertions.assertEquals(1, aggregation.getMinutes().size());
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

        timeToUse = timeToUse.plusDays(73);

        helper.processDayHour(aggregation, timeToUse, "c", timeToUse.minusDays(25), updater);
        Assertions.assertEquals(1, aggregation.getDaysHours().size());
        Assertions.assertEquals("processed-c", aggregation.getDaysHours().getStringObjectMap("9-25").get("17"));

        timeToUse = timeToUse.plusDays(17);

        // Process data for today + 370 days which is little more than 1 year (No new keys are added to aggregation - time is not good)
        helper.processDayHour(aggregation, timeToUse, "d", timeToUse.minusDays(370), updater);
        Assertions.assertEquals(0, aggregation.getDaysHours().size());

        timeToUse = timeToUse.plusDays(17);

        helper.processDayHour(aggregation, timeToUse, "d", timeToUse.minusDays(4), updater);
        Assertions.assertEquals(1, aggregation.getDaysHours().size());
        Assertions.assertEquals("processed-d", aggregation.getDaysHours().getStringObjectMap("11-19").get("17"));

        helper.processDayHour(aggregation, DateTime.now().plusDays(35), "a", DateTime.now().plusDays(30), updater);
        Assertions.assertEquals(1, aggregation.getDaysHours().size());

        System.out.println(JsonUtils.asJson(aggregation));
    }

}