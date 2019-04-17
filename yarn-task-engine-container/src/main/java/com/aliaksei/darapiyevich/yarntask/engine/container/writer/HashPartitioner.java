package com.aliaksei.darapiyevich.yarntask.engine.container.writer;

import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.SchemaUtils;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class HashPartitioner implements Partitioner {
    private final int numPartitions;
    private final List<Integer> partitionKeysIndexes;

    public HashPartitioner(Schema schema,
                           int numPartitions,
                           List<String> partitionKeys) {
        this.numPartitions = numPartitions;
        partitionKeysIndexes = SchemaUtils.getCellIndexes(schema, partitionKeys);
    }

    @Override
    public int getPartition(Record record) {
        Record partitionKey = getPartitionKey(record);
        return partitionKey.hashCode() % numPartitions;
    }

    private Record getPartitionKey(Record record) {
        List<Object> cells = record.getCells();
        List<Object> keyCells = partitionKeysIndexes.stream()
                .map(cells::get)
                .collect(toList());
        return new Record(keyCells);
    }
}
