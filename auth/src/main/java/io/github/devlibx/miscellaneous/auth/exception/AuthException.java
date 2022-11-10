package io.github.devlibx.miscellaneous.auth.exception;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class AuthException extends RuntimeException {
    public AuthException(String msg) {
        super(msg);
    }
}
