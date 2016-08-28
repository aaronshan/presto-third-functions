package cc.shanruifeng.functions.udfs.scalar.date;

import com.facebook.presto.metadata.FunctionListBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import junit.framework.Assert;
import org.junit.Test;

/**
 * @author ruifeng.shan
 * @date 2016-07-12
 * @time 14:32
 */
public class ZodiacSignFunctionsTest {

    @Test
    public void testFunctionCreate() throws Exception {
        FunctionListBuilder builder = new FunctionListBuilder();
        builder.scalars(ZodiacSignFunctions.class);
    }

    @Test
    public void testGetZodiacSignCn() throws Exception {
        Slice result = ZodiacSignFunctions.getZodiacSignCn(Slices.utf8Slice("1989-01-08"));
        Assert.assertEquals("魔羯座", result.toStringUtf8());
    }

    @Test
    public void testGetZodiacSignEn() throws Exception {
        Slice result = ZodiacSignFunctions.getZodiacSignEn(Slices.utf8Slice("1989-01-08"));
        Assert.assertEquals("Capricorn", result.toStringUtf8());
    }
}