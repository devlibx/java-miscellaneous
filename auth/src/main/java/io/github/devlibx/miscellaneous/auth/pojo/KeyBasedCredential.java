package io.github.devlibx.miscellaneous.auth.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.github.devlibx.miscellaneous.auth.ICredentials;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KeyBasedCredential implements ICredentials {
    private String userName;
    private String key;
    private String id;
}
