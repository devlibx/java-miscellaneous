package io.github.devlibx.miscellaneous.util.aggregation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeWindowDataAggregation {
    private final StringObjectMap days = new StringObjectMap();
    private final StringObjectMap hours = new StringObjectMap();
    private final StringObjectMap minutes = new StringObjectMap();
}
