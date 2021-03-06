package onthelive.kr.sttBatch.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import onthelive.kr.sttBatch.batch.listener.NoWorkFoundStepExecutionListener;
import onthelive.kr.sttBatch.batch.stt.step.stt.SpeechToTextProcessorCustom;
import onthelive.kr.sttBatch.entity.OctopusJob;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class BatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    private final Step updateSectionStep;
    private final Step updateSegmentStep;
    private final Step updateSectionUserStep;

    private final SpeechToTextProcessorCustom speechToTextProcessorCustom;

    private static final int CHUNK_SIZE = 1;

    @Value("${project-type-code}")
    private String projectTypeCode;

    @Value("${total-instance-number}")
    private Integer totalInstanceNumber;
    @Value("${instance-number}")
    private Integer instanceNumber;

    // --------------- MultiThread --------------- //

    private static final int DEFAULT_POOL_SIZE = 10;

    public TaskExecutor executor(int poolSize) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        executor.setThreadNamePrefix("multi-thread-");
        executor.setWaitForTasksToCompleteOnShutdown(Boolean.TRUE);
        executor.initialize();
        return executor;
    }


    // --------------- MultiThread --------------- //

    // --------------- Context --------------- //
    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();

        listener.setKeys(new String[] {"jobMasterId", "jobSubId"});

        return listener;
    }
    // --------------- Context --------------- //

    @Bean
    public Job octopusBatchJob() throws Exception {
        return jobBuilderFactory.get("octopusBatchJob")
//                .start(speechToTextStep(DEFAULT_POOL_SIZE)) // TODO ????????? ????????? ???????????? int poolSize ??? ????????????.
//                    .on("FAILED").to(updateJobFailStep).on("*").end()
//                .from(speechToTextStep(DEFAULT_POOL_SIZE)) // TODO ????????? ????????? ???????????? int poolSize ??? ????????????.
//                    .on("*").to(updateSegmentStep).on("*").to(updateSectionStep).end()
                .start(speechToTextStep(DEFAULT_POOL_SIZE))
                .on("*").to(updateSegmentStep)
                .on("*").to(updateSectionStep)
                .on("*").to(updateSectionUserStep)
                .end()
                .incrementer(new RunIdIncrementer()).build();
    }

    // --------------- speechToTextStep() START --------------- //

    @Bean
    @JobScope
    public Step speechToTextStep(@Value("#{jobParameters[poolSize]}") int poolSize) throws Exception {
        log.info("How many threads are there? : " + poolSize);
        return stepBuilderFactory.get("speechToTextStep")
                .<OctopusJob , OctopusJob>chunk(CHUNK_SIZE)
                .reader(speechToTextReader())
                .processor(speechToTextProcessorCustom)
                .writer(compositeItemWriter(
                        speechToTextWriter(),
                        updateJobMastersSetStateCompleteSTT(),
                        insertIntoJobHistoriesSTT(),
                        updateJobSubsSetStateCompleteSTT()
                ))
                .listener(new NoWorkFoundStepExecutionListener())
                .listener(promotionListener())
                .taskExecutor(executor(poolSize))
                .throttleLimit(poolSize)
                .build();
    }

    @Bean
    public JdbcPagingItemReader<OctopusJob> speechToTextReader() throws Exception {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("process_code", "STT");
        parameterValues.put("state" , "WAIT");
        parameterValues.put("state1", "FAIL");
        parameterValues.put("project_type_code", projectTypeCode); // TODO property ??? ????????? ???????????? 16??? ????????? ????????? ??? ?????????!

        parameterValues.put("totalInstanceNumber",totalInstanceNumber);
        parameterValues.put("instanceNumber", instanceNumber);

        return new JdbcPagingItemReaderBuilder<OctopusJob>()
                .pageSize(CHUNK_SIZE)
                .fetchSize(CHUNK_SIZE)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(OctopusJob.class))
                .queryProvider(speechToTextQueryProvider())
                .parameterValues(parameterValues)
                .name("speechToTextReader")
                .saveState(false)
                .build();
    }

    @Bean
    public PagingQueryProvider speechToTextQueryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource); // Database??? ?????? PagingQueryProvider??? ???????????? ??????
        queryProvider.setSelectClause("select b.job_master_id , b.id as job_sub_id ,b.process_code , b.user_id , a.pre_job_id , b.state ,b.reject_state , b.reject_comment ," +
                "b.project_id , b.section_id , b.segment_id , b.created_datetime , b.updated_datetime ,c.value, d.to_lang , d.project_type_code , ifnull(e.history_cnt , 0) as history_cnt");
        queryProvider.setFromClause("from job_masters a inner join job_subs b on a.id = b.job_master_id " +
                "inner join (select a.job_master_id as master_id , a.job_sub_id , a.value from job_sub_results a)c on a.pre_job_id = c.master_id " +
                "inner join (select id as pid, to_lang, project_type_code from projects) d on a.project_id = d.pid " +
                "left outer join (select count(*) as history_cnt, job_master_id as master_id , job_sub_id as sub_id from job_sub_histories group by job_master_id , job_sub_id) e on " +
                "b.job_master_id = e.master_id and b.id = e.sub_id") ;
        queryProvider.setWhereClause("where b.process_code = :process_code and (b.state = :state or b.state = :state1) and d.project_type_code = :project_type_code");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("job_master_id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        return queryProvider.getObject();
    }

    // --------------- speechToTextStep() END --------------- //

    // --------------- CompositeItemWriter() START --------------- //

    @Bean
    public JdbcBatchItemWriter<OctopusJob> speechToTextWriter() {
        return new JdbcBatchItemWriterBuilder<OctopusJob>()
                .dataSource(dataSource)
                .sql("insert into job_sub_results (job_master_id , job_sub_id , value) " +
                        "values (:job_master_id , :job_sub_id , :value) " +
                        "on duplicate key update value = :value, updated_datetime = now()")
                .beanMapped()
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<OctopusJob> insertIntoJobHistoriesSTT() {
        return new JdbcBatchItemWriterBuilder<OctopusJob>()
                .dataSource(dataSource)
                .sql("INSERT INTO job_sub_histories (id, job_master_id, job_sub_id, user_id, process_code, state, reject_state) " +
                        "values (:history_cnt + 1 , :job_master_id , :job_sub_id , :user_id , :process_code , :state , :reject_state) " +
                        "on duplicate key update state = :state")
                .beanMapped()
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<OctopusJob> updateJobMastersSetStateCompleteSTT() {
        return new JdbcBatchItemWriterBuilder<OctopusJob>()
                .dataSource(dataSource)
                .sql("update job_masters set current_state = :state , updated_datetime = now() where id = :job_master_id")
                .beanMapped()
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<OctopusJob> updateJobSubsSetStateCompleteSTT() {
        return new JdbcBatchItemWriterBuilder<OctopusJob>()
                .dataSource(dataSource)
                .sql("update job_subs set state = :state, updated_datetime = now() where job_master_id = :job_master_id and id = :job_sub_id")
                .beanMapped()
                .build();
    }


    @Bean
    public CompositeItemWriter<OctopusJob> compositeItemWriter(
            @Qualifier("speechToTextWriter") JdbcBatchItemWriter<OctopusJob> speechToTextWriter,
            @Qualifier("updateJobMastersSetStateCompleteSTT") JdbcBatchItemWriter<OctopusJob> updateJobsSetStateCompleteSTT,
            @Qualifier("insertIntoJobHistoriesSTT") JdbcBatchItemWriter<OctopusJob> insertIntoJobHistoriesSTT,
            @Qualifier("updateJobSubsSetStateCompleteSTT") JdbcBatchItemWriter<OctopusJob> updateJobSubsSetStateCompleteSTT
    ){
        CompositeItemWriter<OctopusJob> writer = new CompositeItemWriter<>();
        writer.setDelegates(Arrays.asList(speechToTextWriter , updateJobsSetStateCompleteSTT, insertIntoJobHistoriesSTT , updateJobSubsSetStateCompleteSTT));

        return writer;
    }

    // --------------- CompositeItemWriter() END --------------- //

}
