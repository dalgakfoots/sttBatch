package onthelive.kr.sttBatch.entity;

import lombok.Data;

@Data
public class Segment {

    private Long projectId;
    private Long documentId;
    private Long sectionId;
    private Long id; // segmentId
    private Long cnt;
    private Long cnt2;
}
