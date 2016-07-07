package cc.shanruifeng.functions.udfs.scalar.string;

import cc.shanruifeng.functions.udfs.model.ChinaIdArea;
import cc.shanruifeng.functions.udfs.utils.ConfigUtils;
import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author ruifeng.shan
 * @date 2016-07-07
 * @time 18:28
 */
public class ChinaIdCardFunction {
    private static final Map<String, ChinaIdArea> chinaIdAreaMap = ConfigUtils.getIdCardMap();
    private static Logger logger = LoggerFactory.getLogger(ChinaIdCardFunction.class);

    private static ChinaIdArea getCardValue(Slice card) {
        if (card == null) {
            return null;
        }

        String cardString = card.toStringUtf8();
        if (cardString.length() != 18) {
            return null;
        }

        logger.info("#####chinaIdAreaMap is null? {}.##", chinaIdAreaMap == null);
        logger.info("#####chinaIdAreaMap size={}.##", chinaIdAreaMap.size());

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
}
