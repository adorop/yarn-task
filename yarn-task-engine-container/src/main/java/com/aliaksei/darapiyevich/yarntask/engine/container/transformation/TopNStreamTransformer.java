package com.aliaksei.darapiyevich.yarntask.engine.container.transformation;

import com.aliaksei.darapiyevich.yarntask.engine.container.StreamTransformer;
import com.aliaksei.darapiyevich.yarntask.engine.contract.Record;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.SchemaUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.stream.Stream;

public class TopNStreamTransformer implements StreamTransformer {
    private final int limit;
    private final Integer sortColumnIndex;
    private final Comparator<Record> recordsComparator;

    public TopNStreamTransformer(int limit,
                                 Schema schema,
                                 String sortColumn) {
        this.limit = limit;
        sortColumnIndex = SchemaUtils.getCellIndexes(schema, Collections.singletonList(sortColumn))
                .get(0);
        Comparator<Object> cellsComparator = schema.getFields().get(sortColumnIndex).getComparator();
        recordsComparator = recordComparatorAdapter(cellsComparator);
    }

    private Comparator<Record> recordComparatorAdapter(Comparator<Object> cellsComparator) {
        return (record0, record1) -> cellsComparator.compare(extractSortByCell(record0), extractSortByCell(record1));
    }

    @Override
    public Stream<Record> apply(Stream<Record> stream) {
        return stream.sorted(recordsComparator.reversed())
                .limit(limit);
    }

    private Object extractSortByCell(Record record) {
        return record.getCells().get(sortColumnIndex);
    }
}
