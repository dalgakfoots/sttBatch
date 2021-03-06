package onthelive.kr.sttBatch.entity;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class OctopusJob implements Serializable {
    private Long job_master_id;
    private Long job_sub_id;
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

    private String to_lang; // select to_lang from projects
    private String project_type_code; // select project_type_code from projects

    private Long history_cnt;

    private LocalDateTime created_datetime;
    private LocalDateTime updated_datetime;
}
