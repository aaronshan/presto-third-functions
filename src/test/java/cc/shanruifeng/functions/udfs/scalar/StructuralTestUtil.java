package cc.shanruifeng.functions.udfs.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;

/**
 * @author ruifeng.shan
 * @date 2016-07-21
 * @time 14:58
 */
public final class StructuralTestUtil {
    private StructuralTestUtil() {
    }

    public static Block arrayBlockOf(Type elementType, Object... values) {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), values.length);
        for (Object value : values) {
            appendToBlockBuilder(elementType, value, blockBuilder);
        }
        return blockBuilder.build();
    }

    public static Block mapBlockOf(Type keyType, Type valueType, Map<?, ?> value) {
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType), new BlockBuilderStatus(), value.size() * 2);
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            appendToBlockBuilder(keyType, entry.getKey(), blockBuilder);
            appendToBlockBuilder(valueType, entry.getValue(), blockBuilder);
        }
        return blockBuilder.build();
    }
}
