package io.github.devlibx.miscellaneous.auth.store;

import io.github.devlibx.miscellaneous.auth.pojo.KeyBasedCredential;
import io.github.devlibx.miscellaneous.auth.pojo.User;

import java.util.Optional;

public interface IUserStore {
    Optional<User> fetchUserByKeyBasedCredential(KeyBasedCredential keyBasedCredential);
}
