package cc.shanruifeng.functions.udfs.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.TypeParameter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

/**
 * @author ruifeng.shan
 * @date 2016-07-12
 * @time 15:39
 */
@ScalarFunction("is_null")
@Description("Returns TRUE if the argument is NULL")
public class IsNullFunctions {
    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNullSlice(@Nullable @SqlType("T") Slice value) {
        return (value == null);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNullLong(@Nullable @SqlType("T") Long value) {
        return (value == null);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNullDouble(@Nullable @SqlType("T") Double value) {
        return (value == null);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNullBlock(@Nullable @SqlType("T") Block value) {
        return (value == null);
    }
}
