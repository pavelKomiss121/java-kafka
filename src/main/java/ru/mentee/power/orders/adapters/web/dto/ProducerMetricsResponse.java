package ru.mentee.power.orders.adapters.web.dto;

import java.util.Map;

/** Ответ GET /api/v1/orders/metrics. */
public class ProducerMetricsResponse {

    private Totals totals;
    private Map<String, TopicMetrics> topics;

    public ProducerMetricsResponse() {
    }

    public ProducerMetricsResponse(Totals totals, Map<String, TopicMetrics> topics) {
        this.totals = totals;
        this.topics = topics;
    }

    public Totals getTotals() {
        return totals;
    }

    public void setTotals(Totals totals) {
        this.totals = totals;
    }

    public Map<String, TopicMetrics> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, TopicMetrics> topics) {
        this.topics = topics;
    }

    public static class Totals {
        private long success;
        private long failure;

        public Totals() {
        }

        public Totals(long success, long failure) {
            this.success = success;
            this.failure = failure;
        }

        public long getSuccess() {
            return success;
        }

        public void setSuccess(long success) {
            this.success = success;
        }

        public long getFailure() {
            return failure;
        }

        public void setFailure(long failure) {
            this.failure = failure;
        }
    }

    public static class TopicMetrics {
        private long success;
        private long failure;

        public TopicMetrics() {
        }

        public TopicMetrics(long success, long failure) {
            this.success = success;
            this.failure = failure;
        }

        public long getSuccess() {
            return success;
        }

        public void setSuccess(long success) {
            this.success = success;
        }

        public long getFailure() {
            return failure;
        }

        public void setFailure(long failure) {
            this.failure = failure;
        }
    }
}
