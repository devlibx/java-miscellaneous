package io.github.devlibx.miscellaneous.flink.store;

import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

public interface IGenericStateStore {

    default void finish() throws Exception {
    }

    default void open(Configuration parameters) throws Exception {
    }

    /**
     * Persist the state to Store
     */
    void persist(Key key, GenericState state);

    /**
     * Get generic state from Store
     */
    GenericState get(Key key);

    // NoOp impl
    class NoOpGenericStateStore implements IGenericStateStore, Serializable {
        @Override
        public void persist(Key key, GenericState state) {
        }

        @Override
        public GenericState get(Key key) {
            return null;
        }
    }
}
