package io.github.devlibx.miscellaneous.auth.exception;

import io.github.devlibx.miscellaneous.auth.pojo.Action;
import lombok.Data;

@Data
public class ActionNowAllowedOnResourceException extends AuthException {
    private final Action action;

    public ActionNowAllowedOnResourceException(Action action) {
        this.action = action;
    }
}
