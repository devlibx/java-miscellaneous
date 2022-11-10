package io.github.devlibx.miscellaneous.auth.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
@NoArgsConstructor
public class Policy {
    @Builder.Default
    private String version = "v1";

    @Builder.Default
    private List<Statement> statements = new ArrayList<>();

}
