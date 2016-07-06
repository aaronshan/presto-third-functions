package cc.shanruifeng.functions.udfs.scalar.date;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * @author ruifeng.shan
 * @date 2016-07-06
 * @time 17:40
 */
public class DayOfWeekFunction {
    public final static DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");


    @ScalarFunction("dayOfWeek")
    @Description("Returns the day of week from a date string")
    @SqlType(StandardTypes.INTEGER)
    public static Integer dayOfWeek(@SqlType(StandardTypes.VARCHAR) Slice string)
    {
        if (string == null) {
            return null;
        }

        try {
            LocalDate date = LocalDate.parse(string.toStringUtf8(), DEFAULT_DATE_FORMATTER);
            return date.getDayOfWeek();
        } catch (Exception e) {

        }

        return null;
    }
}
