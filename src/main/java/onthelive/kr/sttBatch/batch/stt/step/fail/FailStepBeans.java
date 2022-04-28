package onthelive.kr.sttBatch.batch.stt.step.fail;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class FailStepBeans {

    private final StepBuilderFactory stepBuilderFactory;

    private final FailStepTasklet failStepTasklet;

    // --------------- updateJobStep() START --------------- //

    @Bean
    public Step updateJobFailStep() throws Exception {
        return stepBuilderFactory.get("updateJobFailStep")
                .tasklet(failStepTasklet)
                .build();
    }
}
