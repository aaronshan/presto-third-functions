package cc.shanruifeng.functions.udfs.scalar.array;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.TypeParameter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

/**
 * @author ruifeng.shan
 * @date 2016-07-13
 * @time 10:19
 */
@ScalarFunction("array_union")
@Description("Union elements of the two given arrays")
public class ArrayUnionFunction {
    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block union(
            @TypeParameter("E") Type type,
            @Nullable @SqlType("array(E)") Block leftArray,
            @Nullable @SqlType("array(E)") Block rightArray) {
        if (leftArray == null && rightArray == null) {
            return null;
        }

        int leftArrayCount = (leftArray != null ? leftArray.getPositionCount() : 0);
        int rightArrayCount = (rightArray != null ? rightArray.getPositionCount() : 0);
        TypedSet typedSet = new TypedSet(type, leftArrayCount + rightArrayCount);
        BlockBuilder distinctElementBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), leftArrayCount + rightArrayCount);
        appendTypedArray(leftArray, type, typedSet, distinctElementBlockBuilder);
        appendTypedArray(rightArray, type, typedSet, distinctElementBlockBuilder);

        return distinctElementBlockBuilder.build();
    }

    private static void appendTypedArray(Block array, Type type, TypedSet typedSet, BlockBuilder blockBuilder) {
        if (array != null) {
            for (int i = 0; i < array.getPositionCount(); i++) {
                if (!typedSet.contains(array, i)) {
                    typedSet.add(array, i);
                    type.appendTo(array, i, blockBuilder);
                }
            }
        }
    }

    @SqlType("array(bigint)")
    public static Block bigintUnion(@Nullable @SqlType("array(bigint)") Block leftArray,@Nullable  @SqlType("array(bigint)") Block rightArray) {
        if (leftArray == null && rightArray == null) {
            return null;
        }

        int leftArrayCount = (leftArray != null ? leftArray.getPositionCount() : 0);
        int rightArrayCount = (rightArray != null ? rightArray.getPositionCount() : 0);
        LongSet set = new LongOpenHashSet(leftArrayCount + rightArrayCount);
        BlockBuilder distinctElementBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), leftArrayCount + rightArrayCount);
        appendBigintArray(leftArray, set, distinctElementBlockBuilder);
        appendBigintArray(rightArray, set, distinctElementBlockBuilder);

        return distinctElementBlockBuilder.build();
    }

    private static void appendBigintArray(Block array, LongSet set, BlockBuilder blockBuilder) {
        if (array != null) {
            boolean containsNull = false;
            for (int i = 0; i < array.getPositionCount(); i++) {
                if (!containsNull && array.isNull(i)) {
                    containsNull = true;
                    blockBuilder.appendNull();
                    continue;
                }
                long value = BIGINT.getLong(array, i);
                if (set.add(value)) {
                    BIGINT.writeLong(blockBuilder, value);
                }
            }
        }
    }
}
