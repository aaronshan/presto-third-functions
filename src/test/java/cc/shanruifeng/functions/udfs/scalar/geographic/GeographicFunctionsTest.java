package cc.shanruifeng.functions.udfs.scalar.geographic;

import com.facebook.presto.metadata.FunctionListBuilder;
import org.junit.Test;

/**
 * @author ruifeng.shan
 * @date 2016-07-12
 * @time 15:08
 */
public class GeographicFunctionsTest {
    @Test
    public void testFunctionCreate() throws Exception {
        FunctionListBuilder builder = new FunctionListBuilder();
        builder.scalars(GeographicFunctions.class);
    }
}