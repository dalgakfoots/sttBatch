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
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class SpeechToTextProcessorCustom implements ItemProcessor<OctopusJob, OctopusJob> {

    private final JdbcTemplate jdbcTemplate;
    private final GcpSttService gcpSttService;

    private StepExecution stepExecution;

    @BeforeStep
    public void saveStepExecution(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public OctopusJob process(OctopusJob octopusJob) throws Exception {

        Long masterId = octopusJob.getJob_master_id();
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
//            segments.forEach(
//                    e -> {
//                        String filePath = e.getAudioFile().getFilePath();
//                        String fileName = e.getAudioFile().getStorageFileName();
//                        String destFile = "/Users/dalgakfoot/Documents/HUFS/fileStorage/" + fileName;
//
//
//                        StringBuffer transcript = null;
//
//                        CommonUtil.saveFile(filePath, destFile);
//                        transcript = gcpSttService.makeTranscriptWithSync(destFile, langCode);
//                        OctopusJobResultValue value = new OctopusJobResultValue(e.getIndex(), transcript.toString());
//                        valueResults.add(value);
//
//                    }
//            );
            for (AudioResultSegment e : segments) {
                String filePath = e.getAudioFile().getFilePath();
                String fileName = e.getAudioFile().getStorageFileName();
                String destFile = "/Users/dalgakfoot/Documents/HUFS/fileStorage/" + fileName;
//                String destFile = "/opt/gcpStt/fileStore/"+fileName;

                CommonUtil.saveFile(filePath, destFile);
                StringBuffer transcript = gcpSttService.makeTranscriptWithSync(destFile, langCode);
                OctopusJobResultValue value = new OctopusJobResultValue(e.getIndex(), transcript.toString());
                valueResults.add(value);
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
}
