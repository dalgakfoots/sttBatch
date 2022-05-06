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
        Long jobMasterId = (Long) context.get("jobMasterId");
        Long jobSubId = (Long) context.get("jobSubId");
        Long historyId = getHistoryId(jobMasterId, jobSubId) + 1;
        jdbcTemplate.update("UPDATE job_masters SET current_state = 'FAIL', updated_datetime = now() WHERE id = ?", jobMasterId);
        jdbcTemplate.update("UPDATE job_subs SET state = 'FAIL', updated_datetime = now() WHERE job_master_id = ? and id = ? ", jobMasterId, jobSubId);

        jdbcTemplate.update("INSERT INTO job_sub_histories (id, job_master_id, job_sub_id, user_id, process_code, state, reject_state) " +
                        "SELECT ?, job_master_id , id , user_id ,  process_code, 'FAIL', '0' FROM job_subs WHERE job_master_id = ? AND id = ?"
                , historyId, jobMasterId, jobSubId);

        return RepeatStatus.FINISHED;
    }

    /* PRIVATE METHODS */

    private Long getHistoryId(Long masterId, Long subId) {
        Long historyId = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM job_sub_histories WHERE job_master_id = ? AND job_sub_id = ?", Long.class,
                masterId, subId
        );
        return historyId;
    }

}
