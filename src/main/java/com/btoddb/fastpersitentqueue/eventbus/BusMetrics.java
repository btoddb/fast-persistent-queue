package com.btoddb.fastpersitentqueue.eventbus;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;


/**
 *
 */
public class BusMetrics {
    public static MetricRegistry registry = new MetricRegistry();
    public static JmxReporter reporter;
    static {
        reporter = JmxReporter.forRegistry(registry).inDomain("com.btoddb.fpq").build();
        reporter.start();
    }

}
