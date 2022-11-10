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
public class Statement {
    private Effect effect;
    private List<String> actions;
    private String resource;

    public Statement() {
        effect = Effect.Allow;
        actions = new ArrayList<>();
        resource = "*";
    }
}
