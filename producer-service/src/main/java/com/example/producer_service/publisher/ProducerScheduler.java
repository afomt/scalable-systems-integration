package com.example.producer_service.publisher;

import com.example.producer_service.sources.ProducerSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProducerScheduler {

    private static final Logger log =
            LoggerFactory.getLogger(ProducerScheduler.class);

    private final SourcePublisher sourcePublisher;
    private final List<ProducerSource<?>> sources;

    public ProducerScheduler(SourcePublisher sourcePublisher,
                             List<ProducerSource<?>> sources) {
        this.sourcePublisher = sourcePublisher;
        this.sources = sources;
    }

    @Scheduled(
        fixedRateString = "${integration.publish.rate-ms:300000}",
        initialDelayString = "${integration.publish.initial-delay-ms:5000}"
    )
    public void runAllSources() {
        log.info("🚀 Running {} producer sources", sources.size());

        for (ProducerSource<?> source : sources) {
            sourcePublisher.publish((ProducerSource<Object>) source);
        }
    }
}
