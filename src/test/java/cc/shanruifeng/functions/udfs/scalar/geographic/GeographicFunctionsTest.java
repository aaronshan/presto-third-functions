package cc.shanruifeng.functions.udfs.scalar.geographic;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.type.TypeRegistry;
import org.junit.Test;

/**
 * @author ruifeng.shan
 * @date 2016-07-12
 * @time 15:08
 */
public class GeographicFunctionsTest {
    @Test
    public void testFunctionCreate() throws Exception {
        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionListBuilder builder = new FunctionListBuilder(typeRegistry);
        builder.scalar(GeographicFunctions.class);
    }
}