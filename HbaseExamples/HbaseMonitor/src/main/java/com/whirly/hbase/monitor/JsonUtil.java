package com.whirly.hbase.monitor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: HbaseExamples
 * @description: JSON序列化工具类
 * @author: 赖键锋
 * @create: 2019-02-16 14:27
 **/
public class JsonUtil {
    private static ObjectMapper objMapper = new ObjectMapper();

    static {
        objMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
        objMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,
                false);
    }

    public static String toJson(Object obj) throws IOException {
        return objMapper.writeValueAsString(obj);
    }


    public static String toJson(Object obj, boolean prettyFormat) throws IOException {
        String json = objMapper.writeValueAsString(obj);
        if (prettyFormat) {
            Object jsonObj = objMapper.readValue(json, Object.class);
            json = objMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObj);
        }
        return json;
    }


    public static <T> T fromJson(Class<T> clazz, String json)
            throws IOException {
        return objMapper.readValue(json, clazz);
    }

    public static <T> List<T> fromJsonList(Class<T> clazz, String json)
            throws IOException {
        return objMapper.readValue(json, objMapper.getTypeFactory()
                .constructCollectionType(List.class, clazz));
    }

    public static <K, V> Map<K, V> fromJsonMap(Class<K> keyClazz, Class<V> valueClazz, String json)
            throws IOException {
        return objMapper.readValue(json, objMapper.getTypeFactory()
                .constructMapType(HashMap.class, keyClazz, valueClazz));
    }

}
