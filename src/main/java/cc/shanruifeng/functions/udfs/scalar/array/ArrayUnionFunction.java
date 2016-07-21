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

import java.util.concurrent.atomic.AtomicBoolean;

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
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray) {
        int leftArrayCount = leftArray.getPositionCount();
        int rightArrayCount = rightArray.getPositionCount();
        TypedSet typedSet = new TypedSet(type, leftArrayCount + rightArrayCount);
        BlockBuilder distinctElementBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), leftArrayCount + rightArrayCount);
        appendTypedArray(leftArray, type, typedSet, distinctElementBlockBuilder);
        appendTypedArray(rightArray, type, typedSet, distinctElementBlockBuilder);

        return distinctElementBlockBuilder.build();
    }

    private static void appendTypedArray(Block array, Type type, TypedSet typedSet, BlockBuilder blockBuilder) {
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (!typedSet.contains(array, i)) {
                typedSet.add(array, i);
                type.appendTo(array, i, blockBuilder);
            }
        }
    }

    @SqlType("array(bigint)")
    public static Block bigintUnion(@SqlType("array(bigint)") Block leftArray, @SqlType("array(bigint)") Block rightArray) {
        int leftArrayCount = leftArray.getPositionCount();
        int rightArrayCount = rightArray.getPositionCount();
        LongSet set = new LongOpenHashSet(leftArrayCount + rightArrayCount);
        BlockBuilder distinctElementBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), leftArrayCount + rightArrayCount);
        AtomicBoolean containsNull = new AtomicBoolean(false);
        appendBigintArray(leftArray, containsNull, set, distinctElementBlockBuilder);
        appendBigintArray(rightArray, containsNull, set, distinctElementBlockBuilder);

        return distinctElementBlockBuilder.build();
    }

    private static void appendBigintArray(Block array, AtomicBoolean containsNull, LongSet set, BlockBuilder blockBuilder) {
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (array.isNull(i)) {
                if (!containsNull.get()) {
                    containsNull.set(true);
                    blockBuilder.appendNull();
                }
                continue;
            }
            long value = BIGINT.getLong(array, i);
            if (set.add(value)) {
                BIGINT.writeLong(blockBuilder, value);
            }
        }
    }
}
