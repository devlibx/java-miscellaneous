package io.github.devlibx.miscellaneous.auth.store.impl;

import io.github.devlibx.miscellaneous.auth.pojo.KeyBasedCredential;
import io.github.devlibx.miscellaneous.auth.pojo.User;
import io.github.devlibx.miscellaneous.auth.store.IUserStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class InMemoryUserStore implements IUserStore {
    private final Map<String, User> userMappingByKeyCredential = new HashMap<>();

    @Override
    public Optional<User> fetchUserByKeyBasedCredential(KeyBasedCredential keyBasedCredential) {
        if (userMappingByKeyCredential.containsKey(keyBasedCredential.getKey())) {
            return Optional.ofNullable(userMappingByKeyCredential.get(keyBasedCredential.getKey()));
        }
        return Optional.empty();
    }
}
