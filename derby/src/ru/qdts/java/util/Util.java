package ru.qdts.java.util;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Util {
    public static <T> void println(T info) {
        System.out.println(info);
    }
    public static <T> long stopwatch(Consumer<T> cons, T value) {
        Instant start = Instant.now();
        cons.accept(value);
        Instant end = Instant.now();
        return start.until(end, ChronoUnit.NANOS);
    }
    public static <T> long stopwatch(Actor actor) {
        Instant start = Instant.now();
        actor.act();
        Instant end = Instant.now();
        return start.until(end, ChronoUnit.NANOS);
    }
    public static <T, U> long stopwatch(BiConsumer<T, U> cons, T value1, U value2) {
        Instant start = Instant.now();
        cons.accept(value1, value2);
        Instant end = Instant.now();
        return start.until(end, ChronoUnit.NANOS);
    }
}
