package onthelive.kr.sttBatch.batch.stt.step.fail;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
@Slf4j
public class FailStepTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    private Long failedJobId;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {

        ExecutionContext context = chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext();
        Long temp = (Long) context.get("jobId");
        setFailedJobId(temp);

        jdbcTemplate.update("UPDATE JOBS SET state = 'FAIL' WHERE id = ?"
        , getFailedJobId());

        jdbcTemplate.update("INSERT INTO JOB_HISTORIES (job_id, process_code, user_id, state, created_datetime, updated_datetime) " +
                "SELECT id, process_code, user_id, 'FAIL', now() , now() FROM JOBS WHERE id = ?" , getFailedJobId());

        return RepeatStatus.FINISHED;
    }

    public void setFailedJobId(Long failedJobId) {
        this.failedJobId = failedJobId;
    }

    public Long getFailedJobId() {
        return failedJobId;
    }

}
