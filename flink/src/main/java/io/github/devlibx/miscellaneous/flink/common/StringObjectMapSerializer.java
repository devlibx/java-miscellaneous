package io.github.devlibx.miscellaneous.flink.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;

public class StringObjectMapSerializer extends Serializer<StringObjectMap> {

    @Override
    public void write(Kryo kryo, Output output, StringObjectMap object) {
        try {
            byte[] ser = JsonUtils.asJson(object).getBytes();
            output.writeInt(ser.length, true);
            output.writeBytes(ser);
        } catch (Exception e) {
            throw new RuntimeException("StringObjectMapSerializer: write() failed", e);
        }
    }

    @Override
    public StringObjectMap read(Kryo kryo, Input input, Class<StringObjectMap> type) {
        try {
            int size = input.readInt(true);
            byte[] barr = new byte[size];
            input.readBytes(barr);
            String str = new String(barr);
            return JsonUtils.convertAsStringObjectMap(str);
        } catch (Exception e) {
            throw new RuntimeException("StringObjectMapSerializer: read() failed", e);
        }
    }
}