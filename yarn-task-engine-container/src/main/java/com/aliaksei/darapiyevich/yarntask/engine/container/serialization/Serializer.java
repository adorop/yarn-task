package com.aliaksei.darapiyevich.yarntask.engine.container.serialization;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;

public interface Serializer {
    SerDeResult<byte[]> serialize(Record record);
}
