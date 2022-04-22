package onthelive.kr.sttBatch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class SttBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(SttBatchApplication.class, args);
	}

}
