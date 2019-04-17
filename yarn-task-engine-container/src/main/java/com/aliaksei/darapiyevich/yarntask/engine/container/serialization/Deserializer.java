package com.aliaksei.darapiyevich.yarntask.engine.container.serialization;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;

import java.util.function.Function;

public interface Deserializer extends Function<String, SerDeResult<Record>> {
}
