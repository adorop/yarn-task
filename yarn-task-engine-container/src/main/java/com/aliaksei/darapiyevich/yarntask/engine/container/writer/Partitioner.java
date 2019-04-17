package com.aliaksei.darapiyevich.yarntask.engine.container.writer;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;

public interface Partitioner {
    int getPartition(Record record);
}
