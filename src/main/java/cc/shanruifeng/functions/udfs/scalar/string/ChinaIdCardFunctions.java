package cc.shanruifeng.functions.udfs.scalar.string;

import cc.shanruifeng.functions.udfs.model.ChinaIdArea;
import cc.shanruifeng.functions.udfs.utils.ConfigUtils;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Map;

/**
 * 中国身份证相关函数
 *
 * @author ruifeng.shan
 * @date 2016-07-07
 * @time 18:28
 */
public class ChinaIdCardFunctions {
    private static final Map<String, ChinaIdArea> chinaIdAreaMap = ConfigUtils.getIdCardMap();
    private static int[] weight = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2};    //十七位数字本体码权重
    private static char[] validate = {'1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2'};    //mod11,对应校验码字符值

    private static ChinaIdArea getCardValue(Slice card) {
        if (card == null) {
            return null;
        }

        String cardString = card.toStringUtf8();
        int cardLength = cardString.length();
        //身份证只有15位或18位
        if (cardLength != 15 && cardLength != 18) {
            return null;
        }

        String cardPrefix = cardString.substring(0, 6);
        if (chinaIdAreaMap.containsKey(cardPrefix)) {
            return chinaIdAreaMap.get(cardPrefix);
        }

        return null;
    }

    @ScalarFunction("id_card_province")
    @Description("get china id card province.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getIdCardProvince(@SqlType(StandardTypes.VARCHAR) Slice card) {
        ChinaIdArea chinaIdArea = getCardValue(card);
        if (chinaIdArea != null) {
            return Slices.utf8Slice(chinaIdArea.getProvince());
        }

        return null;
    }

    @ScalarFunction("id_card_city")
    @Description("get china id card city.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getIdCardCity(@SqlType(StandardTypes.VARCHAR) Slice card) {
        ChinaIdArea chinaIdArea = getCardValue(card);
        if (chinaIdArea != null) {
            return Slices.utf8Slice(chinaIdArea.getCity());
        }

        return null;
    }

    @ScalarFunction("id_card_area")
    @Description("get china id card area.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getIdCardArea(@SqlType(StandardTypes.VARCHAR) Slice card) {
        ChinaIdArea chinaIdArea = getCardValue(card);
        if (chinaIdArea != null) {
            return Slices.utf8Slice(chinaIdArea.getArea());
        }

        return null;
    }

    @ScalarFunction("id_card_birthday")
    @Description("get china id card area.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getIdCardBirthday(@SqlType(StandardTypes.VARCHAR) Slice card) {
        if (isValidIdCard(card)) {
            String cardString = card.toStringUtf8();
            int cardLength = cardString.length();
            if (cardLength == 15) {
                return Slices.utf8Slice("19" + cardString.substring(6, 12));
            } else {
                return Slices.utf8Slice(cardString.substring(6, 14));
            }
        }

        return null;
    }

    @ScalarFunction("id_card_gender")
    @Description("get china id card gender.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getIdCardGender(@SqlType(StandardTypes.VARCHAR) Slice card) {
        if (isValidIdCard(card)) {
            String cardString = card.toStringUtf8();
            int cardLength = cardString.length();
            int genderValue;
            if (cardLength == 15) {
                genderValue = cardString.charAt(15) - 48;
            } else {
                genderValue = cardString.charAt(17) - 48;
            }

            if (genderValue % 2 == 0) {
                return Slices.utf8Slice("女");
            } else {
                return Slices.utf8Slice("男");
            }
        }

        return null;
    }

    @ScalarFunction("is_valid_id_card")
    @Description("whether is valid id card or not.")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isValidIdCard(@SqlType(StandardTypes.VARCHAR) Slice card) {
        return isValidIdCard(card.toStringUtf8());
    }

    /**
     * 判断是否是正确的身份证号
     *
     * @param card
     * @return
     */
    private static boolean isValidIdCard(String card) {
        if (!Strings.isNullOrEmpty(card)) {
            int cardLength = card.length();
            //身份证只有15位或18位
            if (cardLength == 18) {
                String card17 = card.substring(0, 17);
                // 前17位必需都是数字
                if (!card17.matches("[0-9]+")) {
                    return false;
                }
                char validateCode = getValidateCode(card17);
                if (validateCode == card.charAt(17)) {
                    return true;
                }
            } else if (cardLength == 15) {
                if (!card.matches("[0-9]+") || getCardValue(Slices.utf8Slice(card)) == null) {
                    return false;
                }
                return true;
            }
        }

        return false;
    }

    /**
     * 获取正确的校验码
     *
     * @param card17 18位身份证前17位
     * @return
     */
    private static char getValidateCode(String card17) {
        int sum = 0, mode = 0;
        for (int i = 0; i < card17.length(); i++) {
            sum = sum + (card17.charAt(i) - 48) * weight[i];
        }
        mode = sum % 11;
        return validate[mode];
    }

    @ScalarFunction("id_card_info")
    @Description("get china id card info.")
    @SqlType(StandardTypes.JSON)
    public static Slice getJsonOfChinaIdCard(@SqlType(StandardTypes.VARCHAR) Slice card) {
        try {
            Map<String, Object> map = Maps.newHashMap();
            ChinaIdArea chinaIdArea = getCardValue(card);
            if (chinaIdArea == null) {
                map.put("province", null);
                map.put("city", null);
                map.put("area", null);
            } else {
                map.put("province", chinaIdArea.getProvince());
                map.put("city", chinaIdArea.getCity());
                map.put("area", chinaIdArea.getArea());
            }

            map.put("gender", getIdCardGender(card).toStringUtf8());
            map.put("valid", isValidIdCard(card));
            ObjectMapper mapper = new ObjectMapper();
            return Slices.utf8Slice(mapper.writeValueAsString(map));
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
