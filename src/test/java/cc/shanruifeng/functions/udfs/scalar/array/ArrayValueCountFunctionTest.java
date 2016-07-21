package cc.shanruifeng.functions.udfs.scalar.array;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BooleanType;
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
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * @author ruifeng.shan
 * @date 2016-07-21
 * @time 12:06
 */
public class ArrayValueCountFunctionTest {
    @Test
    public void testFunctionCreate() throws Exception {
        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionListBuilder builder = new FunctionListBuilder(typeRegistry);
        builder.scalar(ArrayValueCountFunction.class);
    }

    @Test
    public void testValueCountArray() throws Exception {
        //construct ARRAY[ARRAY[3L], ARRAY[1L, 2L], ARRAY[3L]]
        Block arrayBlock = arrayBlockOf(new ArrayType(INTEGER), arrayBlockOf(INTEGER, 3L), arrayBlockOf(INTEGER, 1L, 2L), arrayBlockOf(INTEGER, 3L), null);
        //construct ARRAY[3L]
        Block valueBlock = arrayBlockOf(INTEGER, 3L);

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), false);
        FunctionListBuilder builder = new FunctionListBuilder(typeManager);
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)))).getMethodHandle();
        Assert.assertEquals(2, ArrayValueCountFunction.valueCount(new ArrayType(INTEGER), equalsMethod, arrayBlock, valueBlock));
        Assert.assertEquals(1, ArrayValueCountFunction.valueCount(DOUBLE, equalsMethod, arrayBlock, (Block) null));
    }

    @Test
    public void testValueCountSlice() throws Exception {
        Block arrayBlock = arrayBlockOf(VARCHAR, "13", "18", "13", null);

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), false);
        FunctionListBuilder builder = new FunctionListBuilder(typeManager);
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(VARCHAR, VARCHAR))).getMethodHandle();
        Assert.assertEquals(2, ArrayValueCountFunction.valueCount(VARCHAR, equalsMethod, arrayBlock, Slices.utf8Slice("13")));
        Assert.assertEquals(1, ArrayValueCountFunction.valueCount(DOUBLE, equalsMethod, arrayBlock, (Slice) null));
    }

    @Test
    public void testValueCountLong() throws Exception {
        Block arrayBlock = arrayBlockOf(INTEGER, 13L, 18L, 13L, null);

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), false);
        FunctionListBuilder builder = new FunctionListBuilder(typeManager);
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(INTEGER, INTEGER))).getMethodHandle();
        Assert.assertEquals(2, ArrayValueCountFunction.valueCount(INTEGER, equalsMethod, arrayBlock, 13L));
        Assert.assertEquals(1, ArrayValueCountFunction.valueCount(DOUBLE, equalsMethod, arrayBlock, (Long) null));
    }

    @Test
    public void testValueCountBoolean() throws Exception {
        Block arrayBlock = arrayBlockOf(BOOLEAN, true, true, false, null);

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), false);
        FunctionListBuilder builder = new FunctionListBuilder(typeManager);
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(BOOLEAN, BOOLEAN))).getMethodHandle();
        Assert.assertEquals(1, ArrayValueCountFunction.valueCount(BOOLEAN, equalsMethod, arrayBlock, false));
        Assert.assertEquals(1, ArrayValueCountFunction.valueCount(DOUBLE, equalsMethod, arrayBlock, (Boolean) null));
    }

    @Test
    public void testValueCountDouble() throws Exception {
        Block arrayBlock = arrayBlockOf(DOUBLE, 1.0, 2.0, 3.0, null);

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), false);
        FunctionListBuilder builder = new FunctionListBuilder(typeManager);
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(DOUBLE, DOUBLE))).getMethodHandle();
        Assert.assertEquals(1, ArrayValueCountFunction.valueCount(DOUBLE, equalsMethod, arrayBlock, 1.0));
        Assert.assertEquals(1, ArrayValueCountFunction.valueCount(DOUBLE, equalsMethod, arrayBlock, (Double) null));
    }
}