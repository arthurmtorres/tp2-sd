package br.ufop.bcc362.tp2.config;

import java.util.Locale;

public final class Environment {
    private Environment() {
    }

    public static String text(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    public static int integer(String name, int defaultValue) {
        String value = text(name, Integer.toString(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " deve ser um número inteiro: " + value, exception);
        }
    }

    public static long longInteger(String name, long defaultValue) {
        String value = text(name, Long.toString(defaultValue));
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " deve ser um número inteiro: " + value, exception);
        }
    }

    public static boolean bool(String name, boolean defaultValue) {
        String value = text(name, Boolean.toString(defaultValue)).toLowerCase(Locale.ROOT);
        return switch (value) {
            case "true", "1", "yes", "sim" -> true;
            case "false", "0", "no", "nao", "não" -> false;
            default -> throw new IllegalArgumentException(name + " deve ser true ou false: " + value);
        };
    }
}

