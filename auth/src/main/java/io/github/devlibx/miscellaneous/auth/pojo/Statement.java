package io.github.devlibx.miscellaneous.auth.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Statement {

    @Builder.Default
    private Effect effect = Effect.Allow;

    @Builder.Default
    private List<String> actions = new ArrayList<>();

    @Builder.Default
    private String resource = "*";
}
