package onthelive.kr.sttBatch.batch.stt.step.allComplete.segment;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class UpdateSegmentStepBean {

    private final StepBuilderFactory stepBuilderFactory;
    private final UpdateSegmentStepTasklet updateSegmentStepTasklet;

    @Bean
    public Step updateSegmentStep() throws Exception {
        return stepBuilderFactory.get("updateSegmentStep")
                .tasklet(updateSegmentStepTasklet)
                .build();
    }
}
