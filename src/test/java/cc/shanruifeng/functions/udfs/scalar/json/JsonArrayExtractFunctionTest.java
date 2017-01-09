package cc.shanruifeng.functions.udfs.scalar.json;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.operator.scalar.JsonPath;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import junit.framework.Assert;
import org.junit.Test;

import java.lang.invoke.MethodHandle;

import static cc.shanruifeng.functions.udfs.scalar.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * @author ruifeng.shan
 * @date 2016-07-21
 * @time 16:01
 */
public class JsonArrayExtractFunctionTest {
    @Test
    public void testFunctionCreate() throws Exception {
        FunctionListBuilder builder = new FunctionListBuilder();
        builder.scalars(JsonArrayExtractFunction.class);
    }

    @Test
    public void testVarcharJsonExtract() throws Throwable {
        Block expectArray = arrayBlockOf(VARCHAR, "13", "18", "12");
        Slice inputJson = Slices.utf8Slice("[{\"a\":{\"b\":13}}, {\"a\":{\"b\":18}}, {\"a\":{\"b\":12}}]");
        Block resultArray = JsonArrayExtractFunction.jsonArrayExtract(inputJson, new JsonPath("$.a.b"));

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        FunctionListBuilder builder = new FunctionListBuilder();
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)))).getMethodHandle();
        Assert.assertEquals(true, (boolean) equalsMethod.invokeExact(resultArray, expectArray));
    }
}