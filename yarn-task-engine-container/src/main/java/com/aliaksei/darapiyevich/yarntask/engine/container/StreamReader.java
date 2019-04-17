package com.aliaksei.darapiyevich.yarntask.engine.container;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;

import java.util.function.Supplier;
import java.util.stream.Stream;

public interface StreamReader extends Supplier<Stream<Record>>, AutoCloseable {
}
