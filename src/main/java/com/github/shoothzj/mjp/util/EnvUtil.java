package com.github.shoothzj.mjp.util;

import org.apache.commons.lang3.StringUtils;

public class EnvUtil {

    public static boolean getBooleanVar(String property, String env, boolean defaultVal) {
        String prop = System.getProperty(property);
        if (StringUtils.isNotEmpty(prop)) {
            return Boolean.parseBoolean(prop);
        }
        String envVal = System.getenv(env);
        if (StringUtils.isNotEmpty(envVal)) {
            return Boolean.parseBoolean(envVal);
        }
        return defaultVal;
    }

    public static int getIntVar(String property, String env, int defaultVal) {
        String prop = System.getProperty(property);
        if (StringUtils.isNotEmpty(prop)) {
            return Integer.parseInt(prop);
        }
        String envVal = System.getenv(env);
        if (StringUtils.isNotEmpty(envVal)) {
            return Integer.parseInt(envVal);
        }
        return defaultVal;
    }

    public static float getFloatVar(String property, String env, float defaultVal) {
        String prop = System.getProperty(property);
        if (StringUtils.isNotEmpty(prop)) {
            return Float.parseFloat(prop);
        }
        String envVal = System.getenv(env);
        if (StringUtils.isNotEmpty(envVal)) {
            return Float.parseFloat(envVal);
        }
        return defaultVal;
    }

    public static double getDoubleVar(String property, String env, double defaultVal) {
        String prop = System.getProperty(property);
        if (StringUtils.isNotEmpty(prop)) {
            return Double.parseDouble(prop);
        }
        String envVal = System.getenv(env);
        if (StringUtils.isNotEmpty(envVal)) {
            return Double.parseDouble(envVal);
        }
        return defaultVal;
    }

    public static String getStringVar(String property, String env, String defaultVal) {
        String prop = System.getProperty(property);
        if (StringUtils.isNotEmpty(prop)) {
            return prop;
        }
        String envVal = System.getenv(env);
        if (StringUtils.isNotEmpty(envVal)) {
            return envVal;
        }
        return defaultVal;
    }
}
