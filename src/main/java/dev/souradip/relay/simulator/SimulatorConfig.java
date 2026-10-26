package dev.souradip.relay.simulator;

import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** With the demo profile, two simulated providers start on 8091 and 8092 so nothing needs API keys. */
@Configuration
@Profile("demo")
class SimulatorConfig {

    @Bean(destroyMethod = "stop")
    ProviderSimulator alphaSimulator() {
        return new ProviderSimulator().tokenDelay(Duration.ofMillis(40)).responseTokens(60).start(8091);
    }

    @Bean(destroyMethod = "stop")
    ProviderSimulator betaSimulator() {
        return new ProviderSimulator().tokenDelay(Duration.ofMillis(60)).responseTokens(60).start(8092);
    }
}
