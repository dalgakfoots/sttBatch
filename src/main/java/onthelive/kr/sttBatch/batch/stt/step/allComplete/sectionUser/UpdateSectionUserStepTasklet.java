package onthelive.kr.sttBatch.batch.stt.step.allComplete.sectionUser;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import onthelive.kr.sttBatch.entity.SectionUser;
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
public class UpdateSectionUserStepTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        List<SectionUser> sectionUsers = getSectionUsers();
        if (sectionUsers.size() > 0) {
            sectionUsers.forEach(
                    e -> {
                        if (e.getCnt() != 0 && e.getCnt2() != 0) {
                            if (Objects.equals(e.getCnt(), e.getCnt2())) {
                                updateSectionUsersStateToComplete(e);
                            }
                        }
                    }
            );
        }

        return RepeatStatus.FINISHED;
    }

    /* PRIVATE METHODS */

    private void updateSectionUsersStateToComplete(SectionUser sectionUser) {
        jdbcTemplate.update(
                "UPDATE section_users SET current_state = 'COMPLETE' , updated_datetime = now() " +
                        "WHERE section_id = ? AND project_id = ? AND document_id = ? AND user_id = ? " +
                        "AND process_code = 'STT'",
                sectionUser.getSectionId(), sectionUser.getProjectId(), sectionUser.getDocumentId(),
                sectionUser.getUserId()
        );
    }

    private List<SectionUser> getSectionUsers() {
        return jdbcTemplate.query(
                "select " +
                        " a.section_id as sectionId, " +
                        " a.project_id as projectId, " +
                        " a.document_id as documentId, " +
                        " a.user_id as userId, " +
                        " a.process_code as processCode, " +
                        " ifnull(b.cnt, 0) as cnt, " +
                        " ifnull(c.cnt, 0) as cnt2 " +
                        "from " +
                        " section_users a " +
                        "left outer join " +
                        "( " +
                        " select " +
                        " section_id , " +
                        " project_id , " +
                        " document_id , " +
                        " user_id, " +
                        " count(*) as cnt " +
                        " from " +
                        " job_subs a " +
                        " where " +
                        " process_code = 'STT' " +
                        " group by " +
                        " a.section_id , " +
                        " a.project_id, " +
                        " a.document_id , " +
                        " a.user_id) b on " +
                        " a.section_id = b.section_id " +
                        " and a.project_id = b.project_id " +
                        " and a.document_id = b.document_id " +
                        " and a.user_id = b.user_id " +
                        "left outer join ( " +
                        " select " +
                        " section_id , " +
                        " project_id , " +
                        " document_id , " +
                        " user_id, " +
                        " count(*) as cnt " +
                        " from " +
                        " job_subs a " +
                        " where " +
                        " process_code = 'STT' " +
                        " and state = 'COMPLETE' " +
                        " group by " +
                        " a.section_id , " +
                        " a.project_id, " +
                        " a.document_id , " +
                        " a.user_id) c on " +
                        " a.section_id = c.section_id " +
                        " and a.project_id = c.project_id " +
                        " and a.document_id = c.document_id " +
                        " and a.user_id = c.user_id " +
                        "where " +
                        " a.process_code = 'STT' " +
                        " and a.current_state = 'WAIT'"
                , new BeanPropertyRowMapper<SectionUser>(SectionUser.class)
        );
    }

}
