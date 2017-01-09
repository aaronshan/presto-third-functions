package cc.shanruifeng.functions.udfs.model;

/**
 * @author ruifeng.shan
 * @date 2017-01-09
 * @time 18:56
 */
public enum DayType {
    HOLIDAY("holiday"), WORKDAY("workday");

    private String code;
    private DayType(String code) {
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }
}
