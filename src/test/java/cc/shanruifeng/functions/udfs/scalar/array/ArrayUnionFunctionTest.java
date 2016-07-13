package cc.shanruifeng.functions.udfs.scalar.array;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.type.TypeRegistry;
import junit.framework.Assert;
import org.junit.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * @author ruifeng.shan
 * @date 2016-07-13
 * @time 11:01
 */
public class ArrayUnionFunctionTest {
    @Test
    public void testFunctionCreate() throws Exception {
        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionListBuilder builder = new FunctionListBuilder(typeRegistry);
        builder.scalar(ArrayUnionFunction.class);
    }

    @Test
    public void testUnion() throws Exception {
        BlockBuilder leftArrayBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 100);
        VARCHAR.writeString(leftArrayBuilder, "13");
        VARCHAR.writeString(leftArrayBuilder, "18");

        BlockBuilder rightArrayBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 100);
        VARCHAR.writeString(rightArrayBuilder, "13");
        VARCHAR.writeString(rightArrayBuilder, "20");

        Block block = ArrayUnionFunction.union(VARCHAR, leftArrayBuilder.build(), rightArrayBuilder.build());
        Block block1 = ArrayUnionFunction.union(VARCHAR, null, rightArrayBuilder.build());
        Assert.assertEquals(3, block.getPositionCount());
        Assert.assertEquals(2, block1.getPositionCount());
    }

    @Test
    public void testBigintUnion() throws Exception {
        BlockBuilder leftArrayBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 100);
        BIGINT.writeLong(leftArrayBuilder, 13);
        BIGINT.writeLong(leftArrayBuilder, 18);

        BlockBuilder rightArrayBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 100);
        BIGINT.writeLong(rightArrayBuilder, 13);
        BIGINT.writeLong(rightArrayBuilder, 20);
        Block block = ArrayUnionFunction.bigintUnion(leftArrayBuilder.build(), rightArrayBuilder.build());
        Block block1 = ArrayUnionFunction.bigintUnion(null, rightArrayBuilder.build());
        Assert.assertEquals(3, block.getPositionCount());
        Assert.assertEquals(2, block1.getPositionCount());
    }
}