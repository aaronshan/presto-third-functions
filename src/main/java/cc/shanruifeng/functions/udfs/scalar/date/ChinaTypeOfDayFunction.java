package cc.shanruifeng.functions.udfs.scalar.date;

import cc.shanruifeng.functions.udfs.model.DayType;
import cc.shanruifeng.functions.udfs.utils.ConfigUtils;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.util.Calendar;
import java.util.Map;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static java.util.concurrent.TimeUnit.DAYS;

/**
 * 1: 法定节假日, 2: 正常周末, 3: 正常工作日 4:攒假的工作日
 *
 * @author ruifeng.shan
 * @date 2016-07-15
 * @time 14:44
 */
public class ChinaTypeOfDayFunction {
    public final static DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    public final static Map<String, String> dayMap = ConfigUtils.getDayMap();

    @ScalarFunction("typeOfDay")
    @Description("Returns the type of day from a date string(yyyy-MM-dd)")
    @SqlType(StandardTypes.INTEGER)
    public static long typeOfDay(@SqlType(StandardTypes.VARCHAR) Slice string) {
        if (string == null) {
            return -1;
        }

        String dateStr = string.toStringUtf8();
        try {
            String value = dayMap.get(dateStr);
            if (DayType.HOLIDAY.getCode().equalsIgnoreCase(value)) {
                return 1;
            } else if (DayType.WORKDAY.getCode().equalsIgnoreCase(value)) {
                return 4;
            } else {
                LocalDate date = LocalDate.parse(string.toStringUtf8(), DEFAULT_DATE_FORMATTER);
                if (date.getDayOfWeek() < 6) {
                    return 3;
                } else {
                    return 2;
                }
            }
        } catch (Exception e) {
        }

        return -1;
    }

    @ScalarFunction("typeOfDay")
    @Description("Returns the day of week from a date string")
    @SqlType(StandardTypes.INTEGER)
    public static long typeOfDay(@SqlType(StandardTypes.DATE) long date) {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(DAYS.toMillis(date));
            LocalDate localDate = LocalDate.fromCalendarFields(calendar);

            String dateStr = localDate.toString(DEFAULT_DATE_FORMATTER);
            return typeOfDay(Slices.utf8Slice(dateStr));
        } catch (Exception e) {
        }

        return -1;
    }
}
