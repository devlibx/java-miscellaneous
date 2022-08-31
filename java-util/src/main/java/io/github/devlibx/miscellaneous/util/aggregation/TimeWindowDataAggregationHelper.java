package io.github.devlibx.miscellaneous.util.aggregation;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TimeWindowDataAggregationHelper<T> {
    private final int daysCount;
    private final int hoursCount;
    private final int minutesCount;

    public TimeWindowDataAggregationHelper(int daysCount, int hoursCount, int minutesCount) {
        this.daysCount = daysCount;
        this.hoursCount = hoursCount;
        this.minutesCount = minutesCount;
    }

    public void add(TimeWindowDataAggregation aggregation, DateTime time) {
    }

    public void processDay(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getDayKeys(currentTime);
        Set<String> keysInInput = aggregation.getDays().keySet();
        keysInInput.forEach(keysToRemove::remove);
        keysToRemove.forEach(keyToRemove -> updater.expired(aggregation.getDays(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getMonthOfYear() + "-" + eventTime.getDayOfMonth();
        if (getDayKeys(currentTime).contains(key)) {
            updater.update(aggregation.getDays(), key, event);
        }
    }

    public void processHours(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getHoursKeys(currentTime);
        Set<String> keysInInput = aggregation.getHours().keySet();
        keysInInput.forEach(keysToRemove::remove);
        keysToRemove.forEach(keyToRemove -> updater.expired(aggregation.getHours(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getDayOfMonth() + "-" + eventTime.getHourOfDay();
        if (getHoursKeys(currentTime).contains(key)) {
            updater.update(aggregation.getHours(), key, event);
        }
    }

    public void processMinutes(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getMinuetKeys(currentTime);
        Set<String> keysInInput = aggregation.getMinutes().keySet();
        keysInInput.forEach(keysToRemove::remove);
        keysToRemove.forEach(keyToRemove -> updater.expired(aggregation.getMinutes(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getHourOfDay() + "-" + eventTime.getMinuteOfHour();
        if (getMinuetKeys(currentTime).contains(key)) {
            updater.update(aggregation.getMinutes(), key, event);
        }
    }

    List<String> getDayKeys(DateTime time) {
        DateTime start = time.minusDays(daysCount);
        List<String> keys = new ArrayList<>();
        while (start.isBefore(time)) {
            start = start.plusDays(1);
            String dayKey = start.getMonthOfYear() + "-" + start.getDayOfMonth();
            keys.add(dayKey);
        }
        return keys;
    }

    List<String> getHoursKeys(DateTime time) {
        DateTime start = time.minusHours(hoursCount);
        List<String> keys = new ArrayList<>();
        while (start.isBefore(time)) {
            start = start.plusHours(1);
            String dayKey = start.getDayOfMonth() + "-" + start.getHourOfDay();
            keys.add(dayKey);
        }
        return keys;
    }

    List<String> getMinuetKeys(DateTime time) {
        DateTime start = time.minusMinutes(minutesCount);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            start = start.plusMinutes(1);
            String dayKey = start.getHourOfDay() + "-" + start.getMinuteOfHour();
            keys.add(dayKey);
        }
        return keys;
    }

    interface IAggregationUpdater<T> {
        void update(StringObjectMap data, String key, T event);

        default void expired(StringObjectMap data, String key, T event) {
            data.remove(key);
        }
    }
}
