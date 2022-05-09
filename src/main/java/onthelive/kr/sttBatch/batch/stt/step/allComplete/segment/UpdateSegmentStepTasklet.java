package onthelive.kr.sttBatch.batch.stt.step.allComplete.segment;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import onthelive.kr.sttBatch.entity.Segment;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@RequiredArgsConstructor
@Component
@Slf4j
public class UpdateSegmentStepTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        List<Segment> segments = getSegments();
        if(segments.size() > 0) {
            segments.forEach(
                    e -> {
                        if (e.getCnt() != 0 && e.getCnt2() != 0) {
                            if (Objects.equals(e.getCnt(), e.getCnt2())) {
                                updateSegmentsStateToComplete(e);
                            }
                        }
                    }
            );
        }

        return RepeatStatus.FINISHED;
    }

    /* PRIVATE METHODS */

    private void updateSegmentsStateToComplete(Segment segment) {
        jdbcTemplate.update(
                "UPDATE segments SET current_state = 'COMPLETE', updated_datetime = now() " +
                        "WHERE id = ? AND project_id = ? AND document_id = ? AND section_id = ?",
                segment.getId() , segment.getProjectId() , segment.getDocumentId() , segment.getSectionId()
        );
    }

    private List<Segment> getSegments(){
        return jdbcTemplate.query(
                "select " +
                        "a.id, " +
                        "a.project_id as projectId , " +
                        "a.document_id as documentId , " +
                        "a.section_id as sectionId , " +
                        "count(*) as cnt, " +
                        "c.cnt2 " +
                        "from " +
                        "segments a " +
                        "inner join  " +
                        "job_masters b on " +
                        "a.project_id = b.project_id " +
                        "and a.document_id = b.document_id " +
                        "and a.section_id = b.section_id " +
                        "and a.id = b.segment_id " +
                        "and b.process_code = 'STT' " +
                        "inner join ( " +
                        "select " +
                        "project_id , " +
                        "document_id , " +
                        "section_id , " +
                        "segment_id , " +
                        "process_code , " +
                        "current_state , " +
                        "count(*) as cnt2 " +
                        "from " +
                        "job_masters " +
                        "where " +
                        "process_code = 'STT' " +
                        "and current_state = 'COMPLETE' " +
                        "group by " +
                        "project_id , " +
                        "document_id , " +
                        "section_id , " +
                        "segment_id , " +
                        "process_code , " +
                        "current_state) c on " +
                        "a.project_id = c.project_id " +
                        "and a.document_id = c.document_id " +
                        "and a.section_id = c.section_id " +
                        "and a.id = c.segment_id " +
                        "where a.current_process = 'STT' and a.current_state ='WAIT' " +
                        "group by " +
                        "a.id , " +
                        "a.project_id , " +
                        "a.document_id , " +
                        "a.section_id"
                ,new BeanPropertyRowMapper<Segment>(Segment.class));
    }
}
