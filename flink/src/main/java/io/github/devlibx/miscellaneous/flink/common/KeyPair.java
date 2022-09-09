package io.github.devlibx.miscellaneous.flink.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Strings;
import io.github.devlibx.miscellaneous.flink.store.Key;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class KeyPair {
    private String key;
    private String subKey;
    private String fallbackSubKeyIfSubKeyIsMissing;

    @JsonIgnore
    public KeyPair(String key) {
        this.key = key;
    }

    @JsonIgnore
    public KeyPair(String key, String subKey) {
        this.key = key;
        this.subKey = subKey;
    }

    @JsonIgnore
    public KeyPair(String key, String subKey, String fallbackSubKeyIfSubKeyIsMissing) {
        this.key = key;
        this.subKey = subKey;
        this.fallbackSubKeyIfSubKeyIsMissing = fallbackSubKeyIfSubKeyIsMissing;
    }

    @JsonIgnore
    public String compiledStringKey() {
        if (Strings.isNullOrEmpty(subKey)) {
            return key;
        } else {
            return key + "#" + subKey;
        }
    }

    @JsonIgnore
    public Key buildKey() {
        if (Strings.isNullOrEmpty(subKey)) {
            if (Strings.isNullOrEmpty(fallbackSubKeyIfSubKeyIsMissing)) {
                return Key.builder().key(key).build();
            } else {
                return Key.builder().key(key).subKey(fallbackSubKeyIfSubKeyIsMissing).build();
            }
        } else {
            return Key.builder().key(key).subKey(subKey).build();
        }
    }
}
