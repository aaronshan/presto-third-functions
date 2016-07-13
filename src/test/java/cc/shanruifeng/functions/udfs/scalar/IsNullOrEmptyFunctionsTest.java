package cc.shanruifeng.functions.udfs.scalar;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.type.TypeRegistry;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

/**
 * @author ruifeng.shan
 * @date 2016-07-12
 * @time 15:44
 */
public class IsNullOrEmptyFunctionsTest {

    @Test
    public void testFunctionCreate() throws Exception {
        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionListBuilder builder = new FunctionListBuilder(typeRegistry);
        builder.scalar(IsNullFunctions.class);
    }

    @Test
    public void testIsNullSlice() throws Exception {
        Assert.assertEquals(true, IsNullFunctions.isNullSlice(null));
        Assert.assertEquals(false, IsNullFunctions.isNullSlice(Slices.utf8Slice("test")));
    }

    @Test
    public void testIsNullLong() throws Exception {
        Assert.assertEquals(true, IsNullFunctions.isNullLong(null));
        Assert.assertEquals(false, IsNullFunctions.isNullLong(1l));
    }

    @Test
    public void testIsNullDouble() throws Exception {
        Assert.assertEquals(true, IsNullFunctions.isNullDouble(null));
        Assert.assertEquals(false, IsNullFunctions.isNullDouble(0.0));
    }

    @Test
    public void testIsNullBlock() throws Exception {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 100);
        BIGINT.writeLong(blockBuilder, 13);
        Assert.assertEquals(true, IsNullFunctions.isNullBlock(null));
        Assert.assertEquals(false, IsNullFunctions.isNullBlock(blockBuilder.build()));
    }
}