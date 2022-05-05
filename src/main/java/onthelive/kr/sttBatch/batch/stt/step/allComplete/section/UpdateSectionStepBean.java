package onthelive.kr.sttBatch.batch.stt.step.allComplete.section;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class UpdateSectionStepBean {

    private final StepBuilderFactory stepBuilderFactory;
    private final UpdateSectionStepTasklet updateSectionStepTasklet;

    @Bean
    public Step updateSectionStep() throws Exception {
        return stepBuilderFactory.get("updateSectionStep")
                .tasklet(updateSectionStepTasklet)
                .build();
    }
}
