package com.btoddb.fastpersitentqueue.chronicle;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;


/**
 * Created by burrb009 on 10/4/14.
 */
public class ChronicleMetrics {
    protected MetricRegistry registry = new MetricRegistry();
    protected JmxReporter reporter;

    public ChronicleMetrics() {
        reporter = JmxReporter.forRegistry(registry).inDomain("fpq.chronicle").build();
    }

    public MetricRegistry getRegistry() {
        return registry;
    }
}
