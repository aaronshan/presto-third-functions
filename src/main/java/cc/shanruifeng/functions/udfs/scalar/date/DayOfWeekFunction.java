package cc.shanruifeng.functions.udfs.scalar.date;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;

import static java.util.concurrent.TimeUnit.DAYS;


/**
 * @author ruifeng.shan
 * @date 2016-07-06
 * @time 17:40
 */
public class DayOfWeekFunction {
    public final static DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");

    @ScalarFunction("dayOfWeek")
    @Description("Returns the day of week from a date string(yyyy-MM-dd)")
    @SqlType(StandardTypes.INTEGER)
    public static long dayOfWeek(@SqlType(StandardTypes.VARCHAR) Slice string) {
        if (string == null) {
            return -1;
        }

        try {
            LocalDate localDate = LocalDate.parse(string.toStringUtf8(), DEFAULT_DATE_FORMATTER);
            return localDate.getDayOfWeek();
        } catch (Exception e) {
            return -1;
        }
    }

    @ScalarFunction("dayOfWeek")
    @Description("Returns the day of week from a date string")
    @SqlType(StandardTypes.INTEGER)
    public static long dayOfWeek(@SqlType(StandardTypes.DATE) long date) {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(DAYS.toMillis(date));
            LocalDate localDate = LocalDate.fromCalendarFields(calendar);
            return localDate.getDayOfWeek();
        } catch (Exception e) {
            return -1;
        }
    }
}
