package com.aliaksei.darapiyevich.yarntask.appmaster.configuration;


import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.YarnApplication;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.YarnApplicationConfigurer;
import com.aliaksei.darapiyevich.yarntask.engine.appmaster.configuration.YarnApplicationConfigurerAdapter;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Field;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.PrimitiveType;
import com.aliaksei.darapiyevich.yarntask.engine.contract.schema.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import static com.aliaksei.darapiyevich.yarntask.appmaster.configuration.BookingEventProperties.*;
import static com.aliaksei.darapiyevich.yarntask.engine.contract.aggregation.Aggregations.count;
import static com.aliaksei.darapiyevich.yarntask.engine.contract.predicate.Predicate.PredicateBuilder.column;

@Configuration
public class YarnApplicationMasterConfiguration extends YarnApplicationConfigurerAdapter {
    @Value("${read.path}")
    private String pathIn;
    @Value("${write.path}")
    private String pathOut;
    @Value("${aggregate.parallelism}")
    private int aggregateParallelism;

    @Override
    protected YarnApplication configure(YarnApplicationConfigurer configurer) {
        return configurer.read("csv")
                .inPath(pathIn)
                .withSchema(schema())
                .select(HOTEL_CONTINENT_ID, HOTEL_COUNTRY_ID, HOTEL_MARKET_ID, SEARCH_ADULTS_COUNT, IS_BOOKING)
                .where(column(SEARCH_ADULTS_COUNT).eq(2))
                .where(column(IS_BOOKING).eq(true))
                .aggregate(count().by(HOTEL_CONTINENT_ID, HOTEL_COUNTRY_ID, HOTEL_MARKET_ID).as("count"), aggregateParallelism)
                .top(3, "count")
                .to(pathOut)
                .format("csv")
                .write();
    }

    private Schema schema() {
        return Schema.builder()
                .field(new Field(DATE_TIME, PrimitiveType.DATE_TIME))
                .field(new Field(SITE_ID, PrimitiveType.INTEGER))
                .field(new Field(SITE_CONTINENT_ID, PrimitiveType.INTEGER))
                .field(new Field(USER_LOCATION_COUNTRY_ID, PrimitiveType.INTEGER))
                .field(new Field(USER_LOCATION_REGION_ID, PrimitiveType.INTEGER))
                .field(new Field(USER_LOCATION_CITY_ID, PrimitiveType.INTEGER))
                .field(new Field(ORIG_DESTINATION_DISTANCE, PrimitiveType.DOUBLE))
                .field(new Field(USER_ID, PrimitiveType.INTEGER))
                .field(new Field(IS_MOBILE, PrimitiveType.BOOLEAN))
                .field(new Field(IS_PACKAGE, PrimitiveType.BOOLEAN))
                .field(new Field(CHANNEL_ID, PrimitiveType.INTEGER))
                .field(new Field(SEARCH_CHECK_IN, PrimitiveType.DATE))
                .field(new Field(SEARCH_CHECK_OUT, PrimitiveType.DATE))
                .field(new Field(SEARCH_ADULTS_COUNT, PrimitiveType.INTEGER))
                .field(new Field(SEARCH_CHILDREN_COUNT, PrimitiveType.INTEGER))
                .field(new Field(SEARCH_ROOMS_COUNT, PrimitiveType.INTEGER))
                .field(new Field(SEARCH_DESTINATION_ID, PrimitiveType.INTEGER))
                .field(new Field(SEARCH_DESTINATION_TYPE_ID, PrimitiveType.INTEGER))
                .field(new Field(IS_BOOKING, PrimitiveType.BOOLEAN))
                .field(new Field(SIMILAR_EVENTS_COUNT, PrimitiveType.LONG))
                .field(new Field(HOTEL_CONTINENT_ID, PrimitiveType.INTEGER))
                .field(new Field(HOTEL_COUNTRY_ID, PrimitiveType.INTEGER))
                .field(new Field(HOTEL_MARKET_ID, PrimitiveType.INTEGER))
                .field(new Field(HOTEL_CLUSTER, PrimitiveType.INTEGER))
                .build();
    }
}
