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
import io.github.devlibx.easy.flink.utils.v2.config.AerospikeConfig;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.miscellaneous.flink.store.GenericState;
import io.github.devlibx.miscellaneous.flink.store.IGenericStateStore;
import io.github.devlibx.miscellaneous.flink.store.Key;
import org.joda.time.DateTime;

import java.io.Serializable;


public class AerospikeBackedStateStore implements IGenericStateStore, Serializable {
    private final AerospikeConfig aerospikeConfig;
    private final Configuration configuration;
    private final AerospikeClient aerospikeClient;
    private final boolean throwExceptionOnWriteError;
    private final boolean throwExceptionOnReadError;

    public AerospikeBackedStateStore(AerospikeConfig aerospikeConfig, Configuration configuration) {
        this.aerospikeConfig = aerospikeConfig;
        this.configuration = configuration;

        if (aerospikeConfig.isEnabled() && aerospikeConfig.hosts != null && !aerospikeConfig.hosts.isEmpty()) {
            Host[] hosts = new Host[aerospikeConfig.hosts.size()];
            for (int i = 0; i < aerospikeConfig.hosts.size(); i++) {
                hosts[i] = new Host(aerospikeConfig.hosts.get(i).getHost(), aerospikeConfig.hosts.get(i).getPort());
            }

            throwExceptionOnWriteError = aerospikeConfig.getProperties().getBoolean("throwExceptionOnWriteError", false);
            throwExceptionOnReadError = aerospikeConfig.getProperties().getBoolean("throwExceptionOnReadError", false);

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
            clientPolicy.timeout = aerospikeConfig.getProperties().getInt("timeout", 1000);
            clientPolicy.writePolicyDefault = new WritePolicy();
            clientPolicy.writePolicyDefault.socketTimeout = aerospikeConfig.getProperties().getInt("writePolicy.socketTimeout", 1000);
            clientPolicy.readPolicyDefault = new Policy();
            clientPolicy.readPolicyDefault.socketTimeout = aerospikeConfig.getProperties().getInt("readPolicy.socketTimeout", 1000);
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
            try {
                String finalKey = key.getKey() + "#" + key.getSubKey();

                com.aerospike.client.Key asKey = new com.aerospike.client.Key(aerospikeConfig.namespace, aerospikeConfig.set, finalKey);
                Bin binData = new Bin("data", JsonUtils.asJson(state.getData()));
                Bin binUpdatedAt = new Bin("updated_at", System.currentTimeMillis());

                WritePolicy writePolicy = new WritePolicy();
                writePolicy.setTimeout(aerospikeConfig.getProperties().getInt("writePolicy.timeout", 1000));
                writePolicy.expiration = (int) (state.getTtl().getMillis() - DateTime.now().getMillis()) / 1000;
                aerospikeClient.put(writePolicy, asKey, binData, binUpdatedAt);

            } catch (Exception e) {
                if (throwExceptionOnWriteError) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public GenericState get(Key key) {
        if (aerospikeClient != null) {
            try {
                String finalKey = key.getKey() + "#" + key.getSubKey();
                com.aerospike.client.Key asKey = new com.aerospike.client.Key(aerospikeConfig.namespace, aerospikeConfig.set, finalKey);

                Policy policy = new Policy();
                policy.setTimeout(aerospikeConfig.getProperties().getInt("readPolicy.timeout", 1000));
                Record record = aerospikeClient.get(policy, asKey);
                GenericState result = new GenericState();
                result.setData(JsonUtils.convertAsStringObjectMap(record.getString("data")));
                return result;
            } catch (Exception e) {
                if (throwExceptionOnReadError) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }
}
