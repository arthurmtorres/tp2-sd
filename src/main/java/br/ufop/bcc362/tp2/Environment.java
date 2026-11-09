package br.ufop.bcc362.tp2;

import java.util.Locale;

final class Environment {
    private Environment() {
    }

    static String text(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    static int integer(String name, int defaultValue) {
        try {
            return Integer.parseInt(text(name, Integer.toString(defaultValue)));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " deve ser um número inteiro", exception);
        }
    }

    static long longInteger(String name, long defaultValue) {
        try {
            return Long.parseLong(text(name, Long.toString(defaultValue)));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " deve ser um número inteiro", exception);
        }
    }

    static boolean bool(String name, boolean defaultValue) {
        return switch (text(name, Boolean.toString(defaultValue)).toLowerCase(Locale.ROOT)) {
            case "true", "1", "yes", "sim" -> true;
            case "false", "0", "no", "nao", "não" -> false;
            default -> throw new IllegalArgumentException(name + " deve ser true ou false");
        };
    }
}

