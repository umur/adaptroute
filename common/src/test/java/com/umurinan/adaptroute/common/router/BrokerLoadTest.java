package com.umurinan.adaptroute.common.router;

import com.umurinan.adaptroute.common.dto.BrokerLoad;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link BrokerLoad#computeLoadScore()}.
 */
@DisplayName("BrokerLoad — load score computation")
class BrokerLoadTest {

    @Test
    @DisplayName("Neutral load produces score of 1.0")
    void neutralLoadScoresOne() {
        BrokerLoad load = BrokerLoad.neutral(BrokerTarget.KAFKA);
        assertThat(load.computeLoadScore()).isCloseTo(1.0, within(0.001));
    }

    @Test
    @DisplayName("Unhealthy broker always scores 0.0")
    void unhealthyBrokerScoresZero() {
        BrokerLoad load = BrokerLoad.builder()
                .broker(BrokerTarget.KAFKA)
                .cpuUtilisation(0.0)
                .networkUtilisation(0.0)
                .consumerLag(0)
                .healthy(false)
                .build();
        assertThat(load.computeLoadScore()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("Fully saturated broker scores near 0.0")
    void saturatedBrokerScoresNearZero() {
        BrokerLoad load = BrokerLoad.builder()
                .broker(BrokerTarget.KAFKA)
                .cpuUtilisation(1.0)
                .networkUtilisation(1.0)
                .consumerLag(200_000)
                .healthy(true)
                .build();
        assertThat(load.computeLoadScore()).isLessThan(0.1);
    }

    @Test
    @DisplayName("Load score is always in [0.0, 1.0]")
    void loadScoreIsNormalised() {
        for (double cpu = 0.0; cpu <= 1.0; cpu += 0.1) {
            for (double net = 0.0; net <= 1.0; net += 0.1) {
                BrokerLoad load = BrokerLoad.builder()
                        .broker(BrokerTarget.RABBITMQ)
                        .cpuUtilisation(cpu)
                        .networkUtilisation(net)
                        .consumerLag(0)
                        .healthy(true)
                        .build();
                assertThat(load.computeLoadScore()).isBetween(0.0, 1.0);
            }
        }
    }

    @Test
    @DisplayName("High consumer lag reduces load score")
    void highLagReducesScore() {
        BrokerLoad lowLag = BrokerLoad.builder()
                .broker(BrokerTarget.KAFKA)
                .cpuUtilisation(0.1).networkUtilisation(0.1)
                .consumerLag(0).healthy(true).build();

        BrokerLoad highLag = BrokerLoad.builder()
                .broker(BrokerTarget.KAFKA)
                .cpuUtilisation(0.1).networkUtilisation(0.1)
                .consumerLag(100_000).healthy(true).build();

        assertThat(lowLag.computeLoadScore()).isGreaterThan(highLag.computeLoadScore());
    }
}
