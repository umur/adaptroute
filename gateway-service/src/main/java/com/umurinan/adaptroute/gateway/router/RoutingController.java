package com.umurinan.adaptroute.gateway.router;

import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import com.umurinan.adaptroute.common.dto.RoutingDecision;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST API for the adaptive routing gateway.
 *
 * <p>Producer services submit {@link MessageEnvelope} instances to {@code POST /api/route}.
 * The gateway runs the scoring model and dispatches to the appropriate broker,
 * returning the full {@link RoutingDecision} to the caller for traceability.</p>
 */
@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class RoutingController {

    private final AdaptiveMessageDispatcher dispatcher;

    /**
     * Accepts a message envelope and routes it to the optimal broker.
     *
     * @param envelope the message to route; payload and metadata must be set by the producer
     * @return the routing decision including broker selection and scoring breakdown
     */
    @PostMapping("/route")
    public ResponseEntity<RoutingDecision> route(@RequestBody MessageEnvelope envelope) {
        log.debug("route request: msg={} type={} hint={}",
                envelope.getMessageId(), envelope.getMessageType(), envelope.getRoutingHint());

        RoutingDecision decision = dispatcher.dispatch(envelope);
        return ResponseEntity.ok(decision);
    }

    /**
     * Returns a summary of the routing system status.
     * Used by the experiment runner to verify the gateway is ready before starting a run.
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "service", "gateway-service",
                "status",  "UP",
                "router",  "WeightedScoringRouter"
        ));
    }
}
