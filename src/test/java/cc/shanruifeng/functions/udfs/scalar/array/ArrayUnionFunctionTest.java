package cc.shanruifeng.functions.udfs.scalar.array;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.junit.Test;

import java.lang.invoke.MethodHandle;

import static cc.shanruifeng.functions.udfs.scalar.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * @author ruifeng.shanw
 * @date 2016-07-13
 * @time 11:01
 */
public class ArrayUnionFunctionTest {
    @Test
    public void testFunctionCreate() throws Exception {
        FunctionListBuilder builder = new FunctionListBuilder();
        builder.scalar(ArrayUnionFunction.class);
    }

    @Test
    public void testUnion() throws Throwable {
        //construct ARRAY['13', '18', '13']
        Block leftArray = arrayBlockOf(VARCHAR, "13", "18", "13");
        //construct ARRAY['13', '20']
        Block rightArray = arrayBlockOf(VARCHAR, "13", "20");
        //construct ARRAY['13', '18', '20']
        Block expectArray = arrayBlockOf(VARCHAR, "13", "18", "20");

        Block resultArray = ArrayUnionFunction.union(VARCHAR, leftArray, rightArray);
        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig().setExperimentalSyntaxEnabled(true));
        FunctionListBuilder builder = new FunctionListBuilder();
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)))).getMethodHandle();
        Assert.assertEquals(3, resultArray.getPositionCount());
        Assert.assertEquals(true, (boolean)equalsMethod.invokeExact(resultArray, expectArray));
    }

    @Test
    public void testBigintUnion() throws Throwable {
        Block leftArray = arrayBlockOf(BIGINT, 13, 18, 13);
        Block rightArray = arrayBlockOf(BIGINT, 13, 20);
        Block expectArray = arrayBlockOf(BIGINT, 13, 18, 20);

        Block resultArray = ArrayUnionFunction.bigintUnion(leftArray, rightArray);
        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig().setExperimentalSyntaxEnabled(true));
        FunctionListBuilder builder = new FunctionListBuilder();
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT)))).getMethodHandle();
        Assert.assertEquals(3, resultArray.getPositionCount());
        Assert.assertEquals(true, (boolean)equalsMethod.invokeExact(resultArray, expectArray));
    }
}