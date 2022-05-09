package onthelive.kr.sttBatch.entity;

import lombok.Data;

@Data
public class SectionUser {
    private Long sectionId;
    private Long projectId;
    private Long documentId;
    private Long userId;
    private String processCode;
    private int cnt;
    private int cnt2;
}
