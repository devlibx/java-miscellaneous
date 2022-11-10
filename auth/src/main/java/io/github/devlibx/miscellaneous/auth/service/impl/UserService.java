package io.github.devlibx.miscellaneous.auth.service.impl;

import io.github.devlibx.miscellaneous.auth.ICredentials;
import io.github.devlibx.miscellaneous.auth.exception.InvalidCredentialException;
import io.github.devlibx.miscellaneous.auth.pojo.User;
import io.github.devlibx.miscellaneous.auth.service.IUserService;

import java.util.Optional;

public class UserService implements IUserService {
    @Override
    public Optional<User> fetchUserByCredentials(ICredentials credentials) throws InvalidCredentialException {
        return Optional.empty();
    }
}
