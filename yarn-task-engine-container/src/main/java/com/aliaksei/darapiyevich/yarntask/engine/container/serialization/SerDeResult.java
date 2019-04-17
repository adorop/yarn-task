package com.aliaksei.darapiyevich.yarntask.engine.container.serialization;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
@Getter
public class SerDeResult<T> {
    private T result;
    private Failure failure;

    private SerDeResult(T result) {
        this.result = result;
    }

    private SerDeResult(Failure failure) {
        this.failure = failure;
    }

    public static <T> SerDeResult<T> successful(T result) {
        return new SerDeResult<>(result);
    }

    public static <T> SerDeResult<T> failed(Failure failure) {
        return new SerDeResult<>(failure);
    }

    public boolean isSuccessful() {
        return failure == null;
    }

    @RequiredArgsConstructor
    @ToString
    public static class Failure {
        private final Object input;
        private final String errorMessage;
    }
}
