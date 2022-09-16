package io.github.devlibx.miscellaneous.flink.store.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.AerospikeConfig;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.miscellaneous.flink.store.GenericState;
import io.github.devlibx.miscellaneous.flink.store.IGenericStateStore;
import io.github.devlibx.miscellaneous.flink.store.Key;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.io.Serializable;

@Slf4j
public class AerospikeBackedStateStore implements IGenericStateStore, Serializable {
    private final AerospikeConfig aerospikeConfig;
    private final Configuration configuration;
    private final AerospikeClient aerospikeClient;
    private final boolean throwExceptionOnWriteError;
    private final boolean throwExceptionOnReadError;
    private final StringObjectMap aerospikeExtraProperties;

    public AerospikeBackedStateStore(AerospikeConfig aerospikeConfig, Configuration configuration) {
        this.aerospikeConfig = aerospikeConfig;
        this.configuration = configuration;
        this.aerospikeExtraProperties = aerospikeConfig.getProperties() == null ? new StringObjectMap() : aerospikeConfig.getProperties();

        if (aerospikeConfig.isEnabled() && aerospikeConfig.hosts != null && !aerospikeConfig.hosts.isEmpty()) {
            Host[] hosts = new Host[aerospikeConfig.hosts.size()];
            for (int i = 0; i < aerospikeConfig.hosts.size(); i++) {
                hosts[i] = new Host(aerospikeConfig.hosts.get(i).getHost(), aerospikeConfig.hosts.get(i).getPort());
            }

            throwExceptionOnWriteError = aerospikeExtraProperties.getBoolean("throwExceptionOnWriteError", false);
            throwExceptionOnReadError = aerospikeExtraProperties.getBoolean("throwExceptionOnReadError", false);

            ClientPolicy clientPolicy = new ClientPolicy();
            if (!Strings.isNullOrEmpty(aerospikeConfig.getClusterName())) {
                clientPolicy.clusterName = aerospikeConfig.getClusterName();
            }
            if (!Strings.isNullOrEmpty(aerospikeConfig.getUser())) {
                clientPolicy.user = aerospikeConfig.getUser();
            }
            if (!Strings.isNullOrEmpty(aerospikeConfig.getPassword())) {
                clientPolicy.password = aerospikeConfig.getPassword();
            }
            clientPolicy.timeout = aerospikeExtraProperties.getInt("timeout", 1000);
            clientPolicy.writePolicyDefault = new WritePolicy();
            clientPolicy.writePolicyDefault.socketTimeout = aerospikeExtraProperties.getInt("writePolicy.socketTimeout", 1000);
            clientPolicy.readPolicyDefault = new Policy();
            clientPolicy.readPolicyDefault.socketTimeout = aerospikeExtraProperties.getInt("readPolicy.socketTimeout", 1000);
            aerospikeClient = new AerospikeClient(clientPolicy, hosts);

        } else {
            aerospikeClient = null;
            throwExceptionOnWriteError = false;
            throwExceptionOnReadError = false;
        }
    }

    @Override
    public void persist(Key key, GenericState state) {
        if (aerospikeClient != null) {
            String finalKey = "";
            try {
                finalKey = key.getKey() + "#" + key.getSubKey();

                com.aerospike.client.Key asKey = new com.aerospike.client.Key(aerospikeConfig.namespace, aerospikeConfig.set, finalKey);
                Bin binData = new Bin("data", JsonUtils.asJson(state.getData()));
                Bin binUpdatedAt = new Bin("updated_at", System.currentTimeMillis());

                WritePolicy writePolicy = new WritePolicy();
                if (aerospikeExtraProperties.getBoolean("enable-send-key", false)) {
                    writePolicy.sendKey = true;
                }
                writePolicy.setTimeout(aerospikeConfig.getProperties().getInt("writePolicy.timeout", 1000));

                DateTime now = DateTime.now();
                if (state.getTtl() != null && state.getTtl().isAfter(now)) {
                    writePolicy.expiration = (int) (state.getTtl().getMillis() - now.getMillis()) / 1000;
                }

                aerospikeClient.put(writePolicy, asKey, binData, binUpdatedAt);
                if (aerospikeExtraProperties.getBoolean("debug-aerospike-enabled-write", false)) {
                    log.info("write to AS: key={}, data={}", asKey, binData);
                }

            } catch (Exception e) {
                log.info("failed to write to AS: key={}, data={}", finalKey, state);
                if (throwExceptionOnWriteError) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public GenericState get(Key key) {
        if (aerospikeClient != null) {
            String finalKey = "";
            try {
                finalKey = key.getKey() + "#" + key.getSubKey();
                com.aerospike.client.Key asKey = new com.aerospike.client.Key(aerospikeConfig.namespace, aerospikeConfig.set, finalKey);

                Policy policy = new Policy();
                policy.setTimeout(aerospikeConfig.getProperties().getInt("readPolicy.timeout", 1000));

                Record record = aerospikeClient.get(policy, asKey);
                if (aerospikeExtraProperties.getBoolean("debug-aerospike-enabled-read", false)) {
                    log.info("read from AS: key={}, data={}", finalKey, record);
                }

                GenericState result = new GenericState();
                result.setData(JsonUtils.convertAsStringObjectMap(record.getString("data")));
                return result;
            } catch (Exception e) {
                log.info("failed to read from AS: key={}", finalKey);
                if (throwExceptionOnReadError) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }
}
