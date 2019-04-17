package com.aliaksei.darapiyevich.yarntask.engine.container;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;

import java.util.function.Function;
import java.util.stream.Stream;

public interface StreamTransformer extends Function<Stream<Record>, Stream<Record>> {
}
