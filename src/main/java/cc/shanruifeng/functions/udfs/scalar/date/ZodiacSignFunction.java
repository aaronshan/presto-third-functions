package cc.shanruifeng.functions.udfs.scalar.date;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Calendar;

/**
 * @author ruifeng.shan
 * @date 2016-07-07
 * @time 16:13
 */
public class ZodiacSignFunction {
    public final static DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");

    ;
    private static final String[] zodiacCnArray = {"魔羯座", "水瓶座", "双鱼座", "白羊座", "金牛座", "双子座", "巨蟹座", "狮子座", "处女座", "天秤座", "天蝎座", "射手座"};
    private static final String[] zodiacEnArray = {"Capricorn", "Aquarius", "Pisces", "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo", "Libra", "Scorpio", "Sagittarius"};

    @ScalarFunction("zodiac_cn")
    @Description("from the input date string or separate month and day arguments, returns the chinese string of the Zodiac.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getZodiacSignCn(@SqlType(StandardTypes.VARCHAR) Slice string) {
        if (string == null) {
            return null;
        }

        try {
            LocalDate date = LocalDate.parse(string.toStringUtf8(), DEFAULT_DATE_FORMATTER);
            String zodiac = getZodiac(date.getMonthOfYear(), date.getDayOfMonth(), language.CN);
            return Slices.utf8Slice(zodiac);
        } catch (Exception e) {
            return null;
        }
    }

    @ScalarFunction("zodiac_cn")
    @Description("from the input date string or separate month and day arguments, returns the chinese string of the Zodiac.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getZodiacSignCn(@SqlType(StandardTypes.DATE) long t) {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(t);
            LocalDate date = LocalDate.fromCalendarFields(calendar);
            String zodiac = getZodiac(date.getMonthOfYear(), date.getDayOfMonth(), language.CN);
            return Slices.utf8Slice(zodiac);
        } catch (Exception e) {
            return null;
        }
    }

    @ScalarFunction("zodiac")
    @Description("from the input date string or separate month and day arguments, returns the string of the Zodiac.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getZodiacSignEn(@SqlType(StandardTypes.VARCHAR) Slice string) {
        if (string == null) {
            return null;
        }

        try {
            LocalDate date = LocalDate.parse(string.toStringUtf8(), DEFAULT_DATE_FORMATTER);
            String zodiac = getZodiac(date.getMonthOfYear(), date.getDayOfMonth(), language.EN);
            return Slices.utf8Slice(zodiac);
        } catch (Exception e) {
            return null;
        }
    }

    @ScalarFunction("zodiac")
    @Description("from the input date string or separate month and day arguments, returns the string of the Zodiac.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getZodiacSignEn(@SqlType(StandardTypes.DATE) long t) {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(t);
            LocalDate date = LocalDate.fromCalendarFields(calendar);
            String zodiac = getZodiac(date.getMonthOfYear(), date.getDayOfMonth(), language.EN);
            return Slices.utf8Slice(zodiac);
        } catch (Exception e) {
            return null;
        }
    }

    private static String getZodiac(int month, int day, language language) {
        int[] splitDay = {19, 18, 20, 20, 20, 21, 22, 22, 22, 22, 21, 21}; // split day of two zodiac
        int index = month;
        // if date before the spilt day, idx -1; else idx not changed
        if (day <= splitDay[month - 1]) {
            index = index - 1;
        } else if (month == 12) {
            index = 0;
        }
        // return index's zodiac string
        if (language == ZodiacSignFunction.language.CN) {
            return zodiacCnArray[index];
        } else {
            return zodiacEnArray[index];
        }

    }

    private enum language {CN, EN}
}
