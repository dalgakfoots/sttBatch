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

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class SpeechToTextProcessorCustom implements ItemProcessor<OctopusJob , OctopusJob> {

    private final JdbcTemplate jdbcTemplate;
    private final GcpSttService gcpSttService;

    private StepExecution stepExecution;

    @BeforeStep
    public void saveStepExecution (StepExecution stepExecution){
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
        jdbcTemplate.update("UPDATE JOB_MASTERS SET CURRENT_STATE = 'PROGRESS' WHERE ID = ?", masterId);
        jdbcTemplate.update("UPDATE JOB_SUBS SET STATE = 'PROGRESS' WHERE job_master_id = ? and ID = ? ",masterId, subId);

        jdbcTemplate.update("INSERT INTO JOB_SUB_HISTORIES (id, job_master_id, job_sub_id, user_id, process_code, state, reject_state) " +
                        "VALUES (?, ? , ? , ? , 'STT', 'PROGRESS' , '0')",
                historyId, masterId, subId, octopusJob.getUser_id()
        );
        /**/

        OctopusSoundRecordInfo octopusSoundRecordInfo = new Gson().fromJson(octopusJob.getValue(), OctopusSoundRecordInfo.class);

        String langCode = LangEnum.valueOf(octopusJob.getTo_lang()).getCode();

        List<AudioResultSegment> segments = octopusSoundRecordInfo.getAudioResultBySegment();
        List<OctopusJobResultValue> valueResults = new ArrayList<>();

        segments.forEach(
                e -> {
                    String filePath = e.getAudioFile().getFilePath();
                    String fileName = e.getAudioFile().getStorageFileName();
                    String destFile = "/Users/dalgakfoot/Documents/HUFS/fileStorage/" + fileName;


                    StringBuffer transcript = null;
                    try {
                        CommonUtil.saveFile(filePath , destFile);
                        transcript = gcpSttService.makeTranscriptWithSync(destFile , langCode);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }

                    OctopusJobResultValue value = new OctopusJobResultValue(e.getIndex(), transcript.toString());
                    valueResults.add(value);
                }
        );

        String newValue = new Gson().toJson(valueResults);

        octopusJob.setValue(newValue);
        octopusJob.setUpdated_datetime(LocalDateTime.now());
        octopusJob.setCreated_datetime(LocalDateTime.now());
        octopusJob.setHistory_cnt(historyId);
        return octopusJob;
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
