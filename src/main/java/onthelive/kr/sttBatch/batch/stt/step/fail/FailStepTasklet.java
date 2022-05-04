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
        jdbcTemplate.update("UPDATE JOB_MASTERS SET CURRENT_STATE = 'FAIL' WHERE ID = ?", jobMasterId);
        jdbcTemplate.update("UPDATE JOB_SUBS SET STATE = 'FAIL' WHERE job_master_id = ? and ID = ? ", jobMasterId, jobSubId);

        jdbcTemplate.update("INSERT INTO JOB_SUB_HISTORIES (id, job_master_id, job_sub_id, user_id, process_code, state, reject_state) " +
                        "SELECT ?, job_master_id , id , user_id ,  process_code, 'FAIL', '0' FROM JOB_SUBS WHERE job_master_id = ? AND id = ?"
                , historyId, jobMasterId, jobSubId);

        return RepeatStatus.FINISHED;
    }

    /* PRIVATE METHODS */

    private Long getHistoryId(Long masterId, Long subId) {
        Long historyId = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM JOB_SUB_HISTORIES WHERE job_master_id = ? AND job_sub_id = ?", Long.class,
                masterId, subId
        );
        return historyId;
    }

}
