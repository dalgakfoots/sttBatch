package onthelive.kr.sttBatch.entity;

public enum LangEnum {

    en("en-US"),
    ja("ja-JP"),
    zh("cmn-Hans-CN"),
    es("es-ES"),
    ru("ru-RU"),
    de("de-DE"),
    fr("fr-FR"),
    ko("ko-KR"); // for test

    private String code;

    LangEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }


}
