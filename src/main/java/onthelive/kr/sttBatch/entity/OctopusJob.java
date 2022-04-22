package onthelive.kr.sttBatch.entity;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class OctopusJob {
    private Long id;
    private String process_code;
    private Long user_id;
    private Long pre_job_id;
    private String state;
    private String reject_state;
    private String reject_comment;
    private Long project_id;
    private Long document_id;
    private Long section_id;
    private Long segment_id;

    private String value; // select value from job_results

    private LocalDateTime created_datetime;
    private LocalDateTime updated_datetime;
}
