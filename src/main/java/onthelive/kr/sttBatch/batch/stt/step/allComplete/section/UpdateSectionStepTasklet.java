package onthelive.kr.sttBatch.batch.stt.step.allComplete.section;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import onthelive.kr.sttBatch.entity.Section;
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
public class UpdateSectionStepTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        List<Section> sections = getSections();
        if(sections.size() > 0) {
            sections.forEach(
                    e -> {
                        if(Objects.equals(e.getSegmentCount() , e.getCnt())){
                            updateSectionsStateToComplete(e);
                        }
                    }
            );
        }
        return null;
    }

    /* PRIVATE METHODS */

    private void updateSectionsStateToComplete(Section section) {
        jdbcTemplate.update(
                "UPDATE sections SET current_state = 'COMPLETE', updated_datetime = now() " +
                        "WHERE id = ? AND project_id = ? AND document_id = ?"
                ,section.getId(), section.getProjectId() , section.getDocumentId()
        );
    }

    private List<Section> getSections() {
        return jdbcTemplate.query(
                "select " +
                        "count(*) as cnt, " +
                        "a.project_id , " +
                        "a.document_id , " +
                        "a.id , " +
                        "a.segment_count " +
                        "from " +
                        "sections a " +
                        "inner join segments b ON " +
                        "a.project_id = b.project_id " +
                        "and a.document_id = b.document_id " +
                        "and a.id = b.section_id " +
                        "and b.current_process = 'STT' " +
                        "and b.current_state = 'COMPLETE' " +
                        "where " +
                        "a.current_process = 'STT' " +
                        "and a.current_state = 'WAIT' " +
                        "group by " +
                        "a.id, " +
                        "a.project_id , " +
                        "a.document_id"
                ,new BeanPropertyRowMapper<Section>(Section.class)
        );
    }
}
