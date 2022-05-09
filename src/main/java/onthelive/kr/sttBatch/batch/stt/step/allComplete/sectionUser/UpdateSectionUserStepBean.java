package onthelive.kr.sttBatch.batch.stt.step.allComplete.sectionUser;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class UpdateSectionUserStepBean {
    private final StepBuilderFactory stepBuilderFactory;
    private final UpdateSectionUserStepTasklet updateSectionUserStepTasklet;

    @Bean
    public Step updateSectionUserStep() throws Exception {
        return stepBuilderFactory.get("updateSectionUserStep")
                .tasklet(updateSectionUserStepTasklet)
                .build();
    }
}
