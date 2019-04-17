package com.aliaksei.darapiyevich.yarntask.engine.container;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;

public interface Writer extends AutoCloseable {
    void init();
    void write(Record record);
}
