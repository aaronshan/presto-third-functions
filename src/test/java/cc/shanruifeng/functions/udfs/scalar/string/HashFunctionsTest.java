package cc.shanruifeng.functions.udfs.scalar.string;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.type.TypeRegistry;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author ruifeng.shan
 * @date 2016-07-12
 * @time 11:12
 */
public class HashFunctionsTest {

    @Test
    public void testFunctionCreate() throws Exception {
        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionListBuilder builder = new FunctionListBuilder(typeRegistry);
        builder.scalar(HashFunctions.class);
    }

    @Test
    public void testSha256() throws Exception {
        Slice result = HashFunctions.sha256(Slices.utf8Slice("aaronshan"));
        Assert.assertEquals("d16bb375433ad383169f911afdf45e209eabfcf047ba1faebdd8f6a0b39e0a32", result.toStringUtf8());
    }

    @Test
    public void testMd5() throws Exception {
        Slice result = HashFunctions.md5(Slices.utf8Slice("aaronshan"));
        Assert.assertEquals("95686bc0483262afe170b550dd4544d1", result.toStringUtf8());
    }
}