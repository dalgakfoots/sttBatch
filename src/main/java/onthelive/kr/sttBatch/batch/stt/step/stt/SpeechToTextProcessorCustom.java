package onthelive.kr.sttBatch.batch.stt.step.stt;

import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import onthelive.kr.sttBatch.entity.OctopusJob;
import onthelive.kr.sttBatch.entity.OctopusJobResultValue;
import onthelive.kr.sttBatch.entity.OctopusSoundRecordInfo;
import onthelive.kr.sttBatch.service.gcp.GcpSttService;
import onthelive.kr.sttBatch.util.CommonUtil;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;

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

//        ExecutionContext context = stepExecution.getExecutionContext();
        ExecutionContext context = stepExecution.getJobExecution().getExecutionContext();
        context.put("jobId", octopusJob.getId());

        /* Update JOBS.state : 'WAIT' to 'PROGRESS' begin*/
        jdbcTemplate.update("UPDATE JOBS SET STATE = 'PROGRESS' WHERE ID = ?", octopusJob.getId());
        /* Update JOBS.state : 'WAIT' to 'PROGRESS' end*/

        /* Insert into JOB_HISTORIES begin*/
        // id                   job_id      process_code    user_id     state       created_datetime        updated_datetime
        // auto_increment	    `job_id`	STT	            100020	    PROGRESS	2022-04-25 05:15:08	    2022-04-25 05:15:08

        jdbcTemplate.update
                ("INSERT INTO JOB_HISTORIES (JOB_ID, PROCESS_CODE, USER_ID, STATE, CREATED_DATETIME, UPDATED_DATETIME) " +
                                "VALUES (? , 'STT' , ? , 'PROGRESS' , ? , ?)",
                        octopusJob.getId() , octopusJob.getUser_id() , LocalDateTime.now() , LocalDateTime.now()
                );
        /* Insert into JOB_HISTORIES end*/


        OctopusSoundRecordInfo octopusSoundRecordInfo = new Gson().fromJson(octopusJob.getValue(), OctopusSoundRecordInfo.class);

        System.out.println("octopusSoundRecordInfo = " + octopusSoundRecordInfo);

        String filePath = octopusSoundRecordInfo.getAudioFile().getFilePath();
        String fileName = octopusSoundRecordInfo.getAudioFile().getStorageFileName();
        String destFile = "/Users/dalgakfoot/Documents/HUFS/fileStorage/"+fileName;

        CommonUtil.saveFile(filePath , destFile);

        StringBuffer transcript = gcpSttService.makeTranscriptWithSync(destFile);
        OctopusJobResultValue value = new OctopusJobResultValue(transcript.toString());

        String newValue = new Gson().toJson(value);

        octopusJob.setValue(newValue);
        octopusJob.setUpdated_datetime(LocalDateTime.now());
        octopusJob.setCreated_datetime(LocalDateTime.now());

        return octopusJob;
    }
}
