package onthelive.kr.sttBatch.scheduler;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class GcpSttScheduler {

    private final Job octopusBatchJob;
    private final JobLauncher jobLauncher;

    @Scheduled(fixedDelay = 600 * 1000L)
    public void executeJob() {
        try {
            jobLauncher.run(
                    octopusBatchJob,
                    new JobParametersBuilder().addString("datetime", LocalDateTime.now().toString())
                            .addLong("poolSize", 10L)
                            .toJobParameters()
            );
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
