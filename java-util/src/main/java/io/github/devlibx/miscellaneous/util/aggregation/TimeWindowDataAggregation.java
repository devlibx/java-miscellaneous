package io.github.devlibx.miscellaneous.util.aggregation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class TimeWindowDataAggregation {
    private long updatedAt;
    private final StringObjectMap days = new StringObjectMap();
    private final StringObjectMap daysHours = new StringObjectMap();
    private final StringObjectMap hours = new StringObjectMap();
    private final StringObjectMap minutes = new StringObjectMap();
}
