package cc.shanruifeng.functions.udfs.scalar.map;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.function.OperatorType.EQUAL;

/**
 * @author ruifeng.shan
 * @date 2016-07-20
 * @time 18:19
 */
@ScalarFunction("value_count")
@Description("count numbers if value equals given value.")
public class MapValueCountFunction {

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.INTEGER)
    public static long valueCount(@TypeParameter("V") Type valueType,
                                  @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"}) MethodHandle equals,
                                  @SqlType("map(K,V)") Block mapBlock,
                                  @Nullable @SqlType("V") Block value) {
        int count = 0;
        if (mapBlock.getPositionCount() > 0) {
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                if (value == null || mapBlock.isNull(i + 1)) {
                    if (value == null && mapBlock.isNull(i + 1)) {
                        count++;
                    }
                    continue;
                }

                try {
                    if ((boolean) equals.invokeExact((Block) valueType.getObject(mapBlock, i + 1), value)) {
                        count++;
                    }
                } catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);

                    throw new PrestoException(StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR, t);
                }
            }
        }

        return count;
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.INTEGER)
    public static long valueCount(@TypeParameter("V") Type valueType,
                                  @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"}) MethodHandle equals,
                                  @SqlType("map(K,V)") Block mapBlock,
                                  @Nullable @SqlType("V") Slice value) {
        int count = 0;
        if (mapBlock.getPositionCount() > 0) {
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                if (value == null || mapBlock.isNull(i + 1)) {
                    if (value == null && mapBlock.isNull(i + 1)) {
                        count++;
                    }
                    continue;
                }

                try {
                    if ((boolean) equals.invokeExact(valueType.getSlice(mapBlock, i + 1), value)) {
                        count++;
                    }
                } catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);

                    throw new PrestoException(StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR, t);
                }
            }
        }

        return count;
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.INTEGER)
    public static long valueCount(@TypeParameter("V") Type valueType,
                                  @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"}) MethodHandle equals,
                                  @SqlType("map(K,V)") Block mapBlock,
                                  @Nullable @SqlType("V") Long value) {
        int count = 0;
        if (mapBlock.getPositionCount() > 0) {
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                if (value == null || mapBlock.isNull(i + 1)) {
                    if (value == null && mapBlock.isNull(i + 1)) {
                        count++;
                    }
                    continue;
                }

                try {
                    if ((boolean) equals.invokeExact(valueType.getLong(mapBlock, i + 1), value.longValue())) {
                        count++;
                    }
                } catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);

                    throw new PrestoException(StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR, t);
                }
            }
        }

        return count;
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.INTEGER)
    public static long valueCount(@TypeParameter("V") Type valueType,
                                  @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"}) MethodHandle equals,
                                  @SqlType("map(K,V)") Block mapBlock,
                                  @Nullable @SqlType("V") Boolean value) {
        int count = 0;
        System.out.println(mapBlock.getPositionCount());
        if (mapBlock.getPositionCount() > 0) {
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                if (value == null || mapBlock.isNull(i + 1)) {
                    if (value == null && mapBlock.isNull(i + 1)) {
                        count++;
                    }
                    continue;
                }

                try {
                    if ((boolean) equals.invokeExact(valueType.getBoolean(mapBlock, i + 1), value.booleanValue())) {
                        count++;
                    }
                } catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);

                    throw new PrestoException(StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR, t);
                }
            }
        }

        return count;
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.INTEGER)
    public static long valueCount(@TypeParameter("V") Type valueType,
                                  @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"}) MethodHandle equals,
                                  @SqlType("map(K,V)") Block mapBlock,
                                  @Nullable @SqlType("V") Double value) {
        int count = 0;
        if (mapBlock.getPositionCount() > 0) {
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                if (value == null || mapBlock.isNull(i + 1)) {
                    if (value == null && mapBlock.isNull(i + 1)) {
                        count++;
                    }
                    continue;
                }

                try {
                    if ((boolean) equals.invokeExact(valueType.getDouble(mapBlock, i + 1), value.doubleValue())) {
                        count++;
                    }
                } catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);

                    throw new PrestoException(StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR, t);
                }
            }
        }

        return count;
    }
}
