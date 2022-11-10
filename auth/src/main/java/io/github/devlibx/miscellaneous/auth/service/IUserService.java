package io.github.devlibx.miscellaneous.auth.service;

import io.github.devlibx.miscellaneous.auth.ICredentials;
import io.github.devlibx.miscellaneous.auth.exception.InvalidCredentialException;
import io.github.devlibx.miscellaneous.auth.pojo.User;

import java.util.Optional;

public interface IUserService {
    /**
     * Fetch user by credential
     *
     * @param credentials credential to pull the user object
     * @return user from given credential
     * @throws InvalidCredentialException if credential is invalid or expired
     */
    Optional<User> fetchUserByCredentials(ICredentials credentials) throws InvalidCredentialException;
}
