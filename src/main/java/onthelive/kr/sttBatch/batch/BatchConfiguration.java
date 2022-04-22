package onthelive.kr.sttBatch.batch;

import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import onthelive.kr.sttBatch.entity.OctopusJob;
import onthelive.kr.sttBatch.entity.OctopusJobResultValue;
import onthelive.kr.sttBatch.entity.OctopusSoundRecordInfo;
import onthelive.kr.sttBatch.service.gcp.GcpSttService;
import onthelive.kr.sttBatch.util.CommonUtil;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@RequiredArgsConstructor
public class BatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    private final GcpSttService gcpSttService;

    private static final int CHUNK_SIZE = 50;

    // --------------- MultiThread Test --------------- //

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


    // --------------- MultiThread Test --------------- //

    @Bean
    public Job octopusBatchJob() throws Exception {
        return jobBuilderFactory.get("octopusBatchJob")
                .start(speechToTextStep())
                .next(updateJobStep())
                .next(insertHistoryStep())
                .build();
    }

    // --------------- speechToTextStep() START --------------- //

    @Bean
    public Step speechToTextStep() throws Exception {
        return stepBuilderFactory.get("speechToTextStep")
                .<OctopusJob , OctopusJob>chunk(CHUNK_SIZE)
                .reader(speechToTextReader())
                .processor(speechToTextProcessor())
                .writer(speechToTextWriter())
                .taskExecutor(executor())
                .throttleLimit(poolSize)
                .build();
    }

    @Bean
    public JdbcPagingItemReader<OctopusJob> speechToTextReader() throws Exception {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("process_code", "STT");
        parameterValues.put("state" , "wait");

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
        queryProvider.setSelectClause("select a.* , b.value ");
        queryProvider.setFromClause("from jobs a inner join job_results b on a.pre_job_id  = b.job_id ");
        queryProvider.setWhereClause("where a.process_code = :process_code and a.state = :state");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        return queryProvider.getObject();
    }

    public ItemProcessor<OctopusJob , OctopusJob> speechToTextProcessor() {
        return octopusJob -> {

            OctopusSoundRecordInfo octopusSoundRecordInfo = new Gson().fromJson(octopusJob.getValue(), OctopusSoundRecordInfo.class);

            System.out.println("BatchConfiguration.speechToTextProcessor");
            System.out.println("octopusSoundRecordInfo = " + octopusSoundRecordInfo);

            String filePath = octopusSoundRecordInfo.getFilePath();
            String fileName = octopusSoundRecordInfo.getStorageFileName();
            String destFile = "/Users/dalgakfoot/Documents/HUFS/fileStorage/"+fileName;

            CommonUtil.saveFile(filePath , destFile);

            StringBuffer transcript = gcpSttService.makeTranscriptWithSync(destFile);
            OctopusJobResultValue value = new OctopusJobResultValue(transcript.toString());

            String newValue = new Gson().toJson(value);

            octopusJob.setValue(newValue);
            octopusJob.setUpdated_datetime(LocalDateTime.now());
            octopusJob.setCreated_datetime(LocalDateTime.now());

            return octopusJob;
        };
    }

    @Bean
    public JdbcBatchItemWriter<OctopusJob> speechToTextWriter() {
        return new JdbcBatchItemWriterBuilder<OctopusJob>()
                .dataSource(dataSource)
                .sql("insert into job_results (job_id , value, created_datetime, updated_datetime) values (:id, :value, :created_datetime, :updated_datetime)")
                .beanMapped()
                .build();
    }

    // --------------- speechToTextStep() END --------------- //

    // --------------- updateJobStep() START --------------- //

    @Bean
    public Step updateJobStep() throws Exception {
        return stepBuilderFactory.get("updateJobStep")
                .<OctopusJob , OctopusJob>chunk(CHUNK_SIZE)
                .reader(updateJobReader())
                .writer(updateJobWriter())
                .build();
    }

    @Bean
    public JdbcPagingItemReader<OctopusJob> updateJobReader() throws Exception {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("process_code", "STT");
        parameterValues.put("state" , "wait");

        return new JdbcPagingItemReaderBuilder<OctopusJob>()
                .pageSize(CHUNK_SIZE)
                .fetchSize(CHUNK_SIZE)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(OctopusJob.class))
                .queryProvider(updateJobQueryProvider())
                .parameterValues(parameterValues)
                .name("speechToTextReader")
                .build();
    }

    @Bean
    public PagingQueryProvider updateJobQueryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        queryProvider.setSelectClause("select a.* , b.value ");
        queryProvider.setFromClause("from jobs a inner join job_results b on a.id  = b.job_id ");
        queryProvider.setWhereClause("where a.process_code = :process_code and a.state = :state");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        return queryProvider.getObject();
    }

    @Bean
    public JdbcBatchItemWriter<OctopusJob> updateJobWriter() {
        return new JdbcBatchItemWriterBuilder<OctopusJob>()
                .dataSource(dataSource)
                .sql("update jobs set state = 'complete' where id = :id")
                .beanMapped()
                .build();
    }

    // --------------- updateJobStep() END --------------- //

    // --------------- insertHistoryStep() START --------------- //

    @Bean
    public Step insertHistoryStep() throws Exception {
        return stepBuilderFactory.get("insertHistoryStep")
                .<OctopusJob , OctopusJob>chunk(CHUNK_SIZE)
                .reader(insertHistoryReader())
                .writer(insertHistoryWriter())
                .build();
    }

    @Bean
    public JdbcPagingItemReader<OctopusJob> insertHistoryReader() throws Exception {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("process_code", "STT");
        parameterValues.put("state" , "complete");

        return new JdbcPagingItemReaderBuilder<OctopusJob>()
                .pageSize(CHUNK_SIZE)
                .fetchSize(CHUNK_SIZE)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(OctopusJob.class))
                .queryProvider(insertHistoryQueryProvider())
                .parameterValues(parameterValues)
                .name("speechToTextReader")
                .build();
    }

    @Bean
    public PagingQueryProvider insertHistoryQueryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        queryProvider.setSelectClause("select a.* , b.value ");
        queryProvider.setFromClause("from jobs a inner join job_results b on a.id  = b.job_id left outer join (select job_id from job_histories) c on a.id = c.job_id");
        queryProvider.setWhereClause("where a.process_code = :process_code and a.state = :state and c.job_id is null");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        return queryProvider.getObject();
    }

    @Bean
    public JdbcBatchItemWriter<OctopusJob> insertHistoryWriter() {
        return new JdbcBatchItemWriterBuilder<OctopusJob>()
                .dataSource(dataSource)
                .sql("insert into job_histories (job_id , process_code , user_id , state , created_datetime , updated_datetime)" +
                        "values (:id , :process_code , :user_id , :state , :created_datetime , :updated_datetime) ")
                .beanMapped()
                .build();
    }

    // --------------- insertHistoryStep() END --------------- //

}
