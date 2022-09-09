package io.github.devlibx.miscellaneous.flink.store.ddb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverted;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;
import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.flink.utils.v2.config.DynamoDbConfig;
import io.github.devlibx.miscellaneous.flink.store.GenericState;
import io.github.devlibx.miscellaneous.flink.store.IGenericStateStore;
import io.github.devlibx.miscellaneous.flink.store.Key;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

public class DynamoDBBackedStateStore implements IGenericStateStore, Serializable {
    private DynamoDBMapper dynamoDBMapper;
    private final DynamoDbConfig ddbConfig;
    private final Configuration configuration;

    public DynamoDBBackedStateStore(DynamoDbConfig ddbConfig, Configuration configuration) {
        this.ddbConfig = ddbConfig;
        this.configuration = configuration;
    }

    @Override
    public void persist(Key key, GenericState state) {
        ensureSetupIsDone();

        DdbGenericStateRecord record = new DdbGenericStateRecord();
        record.id = key.getKey();
        if (!Strings.isNullOrEmpty(key.getSubKey())) {
            record.subId = key.getSubKey();
        }
        record.data = JsonUtils.asJson(state.getData());
        record.setUpdatedAt(DateTime.now().toString());
        if (state.getTtl() != null) {
            record.setTtl(new Timestamp(state.getTtl().getMillis()));
        } else {
            record.setTtl(new Timestamp(DateTime.now().plusDays(35).getMillis()));
        }

        try {
            dynamoDBMapper.save(record);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public GenericState get(Key key) {
        ensureSetupIsDone();

        try {
            DdbGenericStateRecord result = dynamoDBMapper.load(DdbGenericStateRecord.class, key.getKey(), key.getSubKey());
            if (result == null) return null;

            return GenericState.builder()
                    .data(JsonUtils.convertAsStringObjectMap(result.getData()))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void ensureSetupIsDone() {
        if (dynamoDBMapper == null) {
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setConnectionTimeout(10000);
            clientConfiguration.setRequestTimeout(10000);
            clientConfiguration.setSocketTimeout(10000);
            clientConfiguration.setClientExecutionTimeout(10000);
            clientConfiguration.setMaxConnections(100);
            AmazonDynamoDB dynamoDB;
            if (!Strings.isNullOrEmpty(ddbConfig.accessKey)) {
                BasicAWSCredentials credentials = new BasicAWSCredentials(ddbConfig.accessKey, ddbConfig.secretKey);
                AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
                dynamoDB = AmazonDynamoDBClientBuilder.standard()
                        .withRegion(Regions.fromName(ddbConfig.region))
                        .withClientConfiguration(clientConfiguration)
                        .withCredentials(credentialsProvider)
                        .build();
            } else {
                dynamoDB = AmazonDynamoDBClientBuilder.standard()
                        .withRegion(Regions.fromName(ddbConfig.region))
                        .withClientConfiguration(clientConfiguration)
                        .build();
            }

            DynamoDBMapperConfig dynamoDBMapperConfig = new DynamoDBMapperConfig.Builder()
                    .withTableNameOverride(DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(ddbConfig.table))
                    .build();
            dynamoDBMapper = new DynamoDBMapper(dynamoDB, dynamoDBMapperConfig);
        }
    }

    @Data
    @NoArgsConstructor
    public static class DdbGenericStateRecord {
        @DynamoDBHashKey
        @DynamoDBAttribute(attributeName = "id")
        private String id;

        @DynamoDBRangeKey
        @DynamoDBAttribute(attributeName = "sub_key")
        private String subId;

        @DynamoDBAttribute(attributeName = "data")
        private String data;

        // @DynamoDBTypeConverted(converter = DynamoDbTimestampConverter.class)
        @DynamoDBAttribute(attributeName = "updated_at")
        private String updatedAt;

        @DynamoDBTypeConverted(converter = DynamoDbTimestampConverter.class)
        @DynamoDBAttribute(attributeName = "ttl")
        private Timestamp ttl;
    }

    public static class DynamoDbTimestampConverter implements DynamoDBTypeConverter<Date, Timestamp> {
        @Override
        public Date convert(final Timestamp timestamp) {
            return new Date(timestamp.getTime());
        }

        @Override
        public Timestamp unconvert(final Date date) {
            return new Timestamp(date.getTime());
        }
    }
}
