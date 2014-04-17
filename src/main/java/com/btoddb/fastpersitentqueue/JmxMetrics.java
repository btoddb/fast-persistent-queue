package com.btoddb.fastpersitentqueue;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


/**
 *
 */
public class JmxMetrics {
    public static final String JMX_ROOT_NAME = "com.btoddb.fpq";

    private final MetricRegistry metricsRegistry = new MetricRegistry();
    private JmxReporter reporter;

    public Meter pushes;
    public Meter pops;

    private final Fpq fpq;

    public JmxMetrics(Fpq fpq) {
        this.fpq = fpq;
    }

    public void init() {
        reporter = JmxReporter.forRegistry(metricsRegistry).inDomain(JMX_ROOT_NAME+"."+fpq.getQueueName()).build();
        reporter.start();

        pushes = metricsRegistry.meter(MetricRegistry.name("pushes"));
        pops = metricsRegistry.meter(MetricRegistry.name("pops"));
        metricsRegistry.register(MetricRegistry.name("size"),
                                 new Gauge<Long>() {
                             @Override
                             public Long getValue() {
                                 return fpq.getNumberOfEntries();
                             }
                         }
        );
//        metricsRegistry.register(MetricRegistry.name("numberOfPushes"),
//                                 new Gauge<Long>() {
//                                     @Override
//                                     public Long getValue() {
//                                         return fpq.getNumberOfPushes();
//                                     }
//                                 }
//        );
//        metricsRegistry.register(MetricRegistry.name("numberOfPops"),
//                                 new Gauge<Long>() {
//                                     @Override
//                                     public Long getValue() {
//                                         return fpq.getNumberOfPops();
//                                     }
//                                 }
//        );

//        metricsRegistry.register(MetricRegistry.name(JMX_ROOT_NAME, fpq, "rollbacks"),
//                         new Gauge<Long>() {
//                             @Override
//                             public Long getValue() {
//                                 return getNumberOfRollbacks();
//                             }
//                         });
    }

}
