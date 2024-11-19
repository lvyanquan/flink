package org.apache.flink.api.connector.source.util.ratelimit;

public interface SupportsRateLimiting {

    RateLimiterStrategy rateLimiterStrategy();
}
