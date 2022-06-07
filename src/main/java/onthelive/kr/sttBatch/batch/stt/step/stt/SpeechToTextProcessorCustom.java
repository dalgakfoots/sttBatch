package onthelive.kr.sttBatch.batch.stt.step.stt;

import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import onthelive.kr.sttBatch.entity.*;
import onthelive.kr.sttBatch.service.gcp.GcpSttService;
import onthelive.kr.sttBatch.util.CommonUtil;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
@Component
public class SpeechToTextProcessorCustom implements ItemProcessor<OctopusJob, OctopusJob> {

    private final JdbcTemplate jdbcTemplate;
    private final GcpSttService gcpSttService;

    private StepExecution stepExecution;

    @Value("${dest-file}")
    private String fileStore;

    @Value("${total-instance-number}")
    private Integer totalInstanceNumber;
    @Value("${instance-number}")
    private Integer instanceNumber;

    @BeforeStep
    public void saveStepExecution(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public OctopusJob process(OctopusJob octopusJob) throws Exception {


        Long masterId = octopusJob.getJob_master_id();
        boolean isStart = checkMasterId(masterId); // Processor 를 실행 시킬 것인가?

        if(isStart) {
            Long subId = octopusJob.getJob_sub_id();
            Long historyId = getHistoryId(masterId, subId) + 1;

            ExecutionContext context = stepExecution.getJobExecution().getExecutionContext();
            context.put("jobMasterId", masterId);
            context.put("jobSubId", subId);


            /**/
            jdbcTemplate.update("UPDATE job_masters SET current_state = 'PROGRESS', updated_datetime = now() WHERE id = ?", masterId);
            jdbcTemplate.update("UPDATE job_subs SET state = 'PROGRESS', updated_datetime = now() WHERE job_master_id = ? and id = ? ", masterId, subId);

            jdbcTemplate.update("INSERT INTO job_sub_histories (id, job_master_id, job_sub_id, user_id, process_code, state, reject_state) " +
                            "VALUES (?, ? , ? , ? , 'STT', 'PROGRESS' , '0')",
                    historyId, masterId, subId, octopusJob.getUser_id()
            );
            /**/

            OctopusSoundRecordInfo octopusSoundRecordInfo = new Gson().fromJson(octopusJob.getValue(), OctopusSoundRecordInfo.class);

            String langCode = LangEnum.valueOf(octopusJob.getTo_lang()).getCode();

            List<AudioResultSegment> segments = octopusSoundRecordInfo.getAudioResultBySegment();
            List<OctopusJobResultValue> valueResults = new ArrayList<>();
            try {

                for (AudioResultSegment e : segments) {
                    String filePath = e.getAudioFile().getFilePath();
                    String fileName = e.getAudioFile().getStorageFileName();
                    String destFile = fileStore + fileName;

                    CommonUtil.saveFile(filePath, destFile);
                    StringBuffer transcript = gcpSttService.makeTranscriptWithSync(destFile, langCode);
                    OctopusJobResultValue value = new OctopusJobResultValue(e.getIndex(), transcript.toString());
                    valueResults.add(value);
                    CommonUtil.deleteFile(destFile);
                }
            } catch (Exception ex) {
                log.info("failed masterId: " + masterId);
                log.info("failed subId: " + subId);
                log.info("failed historyId: " + historyId);
                log.info("failed octopusJob.getUSer_id() : " + octopusJob.getUser_id());
                ex.printStackTrace();
                failProcess(masterId, subId, historyId, octopusJob.getUser_id());
                octopusJob.setState("FAIL");
                octopusJob.setValue("ERROR");
                octopusJob.setUpdated_datetime(LocalDateTime.now());
                octopusJob.setCreated_datetime(LocalDateTime.now());
                octopusJob.setHistory_cnt(historyId);
                return octopusJob;
            }

            String newValue = new Gson().toJson(valueResults);
            octopusJob.setState("COMPLETE");
            octopusJob.setValue(newValue);
            octopusJob.setUpdated_datetime(LocalDateTime.now());
            octopusJob.setCreated_datetime(LocalDateTime.now());
            octopusJob.setHistory_cnt(historyId);
            return octopusJob;
        } else {
            // TODO 프로세서를 실행 안시킬 경우?
            octopusJob.setState("WAIT");
            octopusJob.setValue("");
            octopusJob.setUpdated_datetime(LocalDateTime.now());
            octopusJob.setCreated_datetime(LocalDateTime.now());
            octopusJob.setHistory_cnt(0L);
            return octopusJob;
        }
    }

    /* PRIVATE METHODS */

    private Long getHistoryId(Long masterId, Long subId) {
        Long historyId = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM job_sub_histories WHERE job_master_id = ? AND job_sub_id = ?", Long.class,
                masterId, subId
        );
        return historyId;
    }

    private void failProcess(Long masterId, Long subId, Long historyId, Long userId) {
        log.info("failProcess.....");

        jdbcTemplate.update("UPDATE job_masters SET current_state = 'FAIL', updated_datetime = now() WHERE id = ?", masterId);
        jdbcTemplate.update("UPDATE job_subs SET state = 'FAIL', updated_datetime = now() WHERE job_master_id = ? and id = ? ", masterId, subId);

        jdbcTemplate.update("INSERT INTO job_sub_histories (id, job_master_id, job_sub_id, user_id, process_code, state, reject_state) " +
                        "VALUES (?, ? , ? , ? , 'STT', 'FAIL' , '0') on duplicate key update state = 'FAIL'",
                historyId + 1, masterId, subId, userId
        );
    }

    private boolean checkMasterId(Long masterId) {
        if(totalInstanceNumber != null && instanceNumber != null) {
            return true;
        } else {
            return true;
        }
    }

}
