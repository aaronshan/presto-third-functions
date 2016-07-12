package cc.shanruifeng.functions.udfs.scalar.string;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.type.TypeRegistry;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import junit.framework.Assert;
import org.junit.Test;

/**
 * @author ruifeng.shan
 * @date 2016-07-12
 * @time 14:49
 */
public class ChineseToPinYinFunctionsTest {

    @Test
    public void testFunctionCreate() throws Exception {
        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionListBuilder builder = new FunctionListBuilder(typeRegistry);
        builder.scalar(ChineseToPinYinFunctions.class);
    }

    @Test
    public void testConvertToPinYin() throws Exception {
        Slice result = ChineseToPinYinFunctions.convertToPinYin(Slices.utf8Slice("中国"));
        Assert.assertEquals("zhongguo", result.toStringUtf8());
    }
}