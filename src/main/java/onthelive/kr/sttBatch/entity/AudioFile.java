package onthelive.kr.sttBatch.entity;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class AudioFile {

    private String originFileName;
    private String storageFileName;
//    private String recordLang;
    private String filePath;
    private String fileType;
    private String fileSize;
//    private String recordTime;

}
