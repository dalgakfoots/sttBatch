package onthelive.kr.sttBatch.batch;

import lombok.RequiredArgsConstructor;
import onthelive.kr.sttBatch.batch.stt.step.stt.SpeechToTextProcessorCustom;
import onthelive.kr.sttBatch.entity.OctopusJob;
import onthelive.kr.sttBatch.service.gcp.GcpSttService;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class BatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final JdbcTemplate jdbcTemplate;
    private final DataSource dataSource;

    private final Step updateJobFailStep;

    private final GcpSttService gcpSttService;

    private static final int CHUNK_SIZE = 50;

    // --------------- MultiThread --------------- //

    private int poolSize;

    @Value("${poolSize:10}")
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    @Bean(name = "octopusBatchJobTaskPool")
    public TaskExecutor executor() {
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

        listener.setKeys(new String[] {"jobId"});

        return listener;
    }
    // --------------- Context --------------- //

    @Bean
    public Job octopusBatchJob() throws Exception {
        return jobBuilderFactory.get("octopusBatchJob")
                .start(speechToTextStep())
                    .on("FAILED").to(updateJobFailStep).on("*").end()
                .from(speechToTextStep())
                    .on("*").end()
                .end().incrementer(new RunIdIncrementer()).build();
    }

    // --------------- speechToTextStep() START --------------- //

    @Bean
    public Step speechToTextStep() throws Exception {
        return stepBuilderFactory.get("speechToTextStep")
                .<OctopusJob , OctopusJob>chunk(CHUNK_SIZE)
                .reader(speechToTextReader())
                .processor(new SpeechToTextProcessorCustom(jdbcTemplate , gcpSttService))
                .writer(compositeItemWriter(
                        speechToTextWriter(),
                        updateJobsSetStateCompleteSTT(),
                        insertIntoJobHistoriesSTT()
                ))
                .listener(promotionListener())
                .taskExecutor(executor())
                .throttleLimit(poolSize)
                .build();
    }

    @Bean
    public JdbcPagingItemReader<OctopusJob> speechToTextReader() throws Exception {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("process_code", "STT");
        parameterValues.put("state" , "WAIT");
        parameterValues.put("state1", "FAIL");

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
        queryProvider.setDataSource(dataSource); // Database에 맞는 PagingQueryProvider를 선택하기 위해
        queryProvider.setSelectClause("select a.* , b.value , c.to_lang");
        queryProvider.setFromClause("from jobs a inner join job_results b on a.pre_job_id  = b.job_id" +
                " inner join (select id as pid, to_lang from projects) c on a.project_id = c.pid");
        queryProvider.setWhereClause("where a.process_code = :process_code and (a.state = :state or a.state = :state1)");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        return queryProvider.getObject();
    }

    // --------------- speechToTextStep() END --------------- //

    // --------------- CompositeItemWriter() START --------------- //

    @Bean
    public JdbcBatchItemWriter<OctopusJob> speechToTextWriter() {
        return new JdbcBatchItemWriterBuilder<OctopusJob>()
                .dataSource(dataSource)
                .sql("insert into job_results (job_id , value, created_datetime, updated_datetime) values (:id, :value, :created_datetime, :updated_datetime) " +
                        "on duplicate key update value = :value")
                .beanMapped()
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<OctopusJob> insertIntoJobHistoriesSTT() {
        return new JdbcBatchItemWriterBuilder<OctopusJob>()
                .dataSource(dataSource)
                .sql("insert into job_histories (job_id , process_code , user_id , state , created_datetime , updated_datetime)" +
                        "values (:id , :process_code , :user_id , 'COMPLETE' , :created_datetime , :updated_datetime) ")
                .beanMapped()
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<OctopusJob> updateJobsSetStateCompleteSTT() {
        return new JdbcBatchItemWriterBuilder<OctopusJob>()
                .dataSource(dataSource)
                .sql("update jobs set state = 'COMPLETE' where id = :id")
                .beanMapped()
                .build();
    }



    @Bean
    public CompositeItemWriter<OctopusJob> compositeItemWriter(
            @Qualifier("speechToTextWriter") JdbcBatchItemWriter<OctopusJob> speechToTextWriter,
            @Qualifier("updateJobsSetStateCompleteSTT") JdbcBatchItemWriter<OctopusJob> updateJobsSetStateCompleteSTT,
            @Qualifier("insertIntoJobHistoriesSTT") JdbcBatchItemWriter<OctopusJob> insertIntoJobHistoriesSTT
    ){
        CompositeItemWriter<OctopusJob> writer = new CompositeItemWriter<>();
        writer.setDelegates(Arrays.asList(speechToTextWriter , updateJobsSetStateCompleteSTT, insertIntoJobHistoriesSTT));

        return writer;
    }

    // --------------- CompositeItemWriter() END --------------- //

}
