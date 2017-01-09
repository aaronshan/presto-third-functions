package cc.shanruifeng.functions.udfs.scalar.map;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import junit.framework.Assert;
import org.junit.Test;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static cc.shanruifeng.functions.udfs.scalar.StructuralTestUtil.arrayBlockOf;
import static cc.shanruifeng.functions.udfs.scalar.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * @author ruifeng.shan
 * @date 2016-07-21
 * @time 15:36
 */
public class MapValueCountFunctionTest {
    @Test
    public void testFunctionCreate() throws Exception {
        FunctionListBuilder builder = new FunctionListBuilder();
        builder.scalar(MapValueCountFunction.class);
    }

    @Test
    public void testValueCount() throws Exception {
        //construct map(ARRAY[1,2,3], ARRAY[ARRAY[3L],ARRAY[1L, 2L], ARRAY[3L]])
        Map<Integer, Block> value = Maps.newHashMap();
        value.put(1, arrayBlockOf(INTEGER, 3L));
        value.put(2, arrayBlockOf(INTEGER, 1L, 2L));
        value.put(3, arrayBlockOf(INTEGER, 3L));
        value.put(4, null);
        Block mapBlock = mapBlockOf(INTEGER, new ArrayType(INTEGER), value);
        //construct ARRAY[3L]
        Block valueBlock = arrayBlockOf(INTEGER, 3L);
        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());

        FunctionListBuilder builder = new FunctionListBuilder();
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)))).getMethodHandle();
        Assert.assertEquals(2, MapValueCountFunction.valueCount(new ArrayType(INTEGER), equalsMethod, mapBlock, valueBlock));
        Assert.assertEquals(1, MapValueCountFunction.valueCount(new ArrayType(INTEGER), equalsMethod, mapBlock, (Block) null));
    }

    @Test
    public void testValueCount1() throws Exception {
        //construct map(ARRAY[1,2,3], ARRAY["13", "18", "13"])
        Map<Integer, Slice> value = Maps.newHashMap();
        value.put(1, Slices.utf8Slice("13"));
        value.put(2, Slices.utf8Slice("18"));
        value.put(3, Slices.utf8Slice("13"));
        value.put(4, null);
        Block mapBlock = mapBlockOf(INTEGER, VARCHAR, value);

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        FunctionListBuilder builder = new FunctionListBuilder();
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(VARCHAR, VARCHAR))).getMethodHandle();
        Assert.assertEquals(2, MapValueCountFunction.valueCount(VARCHAR, equalsMethod, mapBlock, Slices.utf8Slice("13")));
        Assert.assertEquals(1, MapValueCountFunction.valueCount(VARCHAR, equalsMethod, mapBlock, (Slice) null));
    }

    @Test
    public void testValueCount2() throws Exception {
        Map<Integer, Long> value = Maps.newHashMap();
        value.put(1, 13L);
        value.put(2, 18L);
        value.put(3, 13L);
        value.put(4, null);
        Block mapBlock = mapBlockOf(INTEGER, INTEGER, value);

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        FunctionListBuilder builder = new FunctionListBuilder();
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(INTEGER, INTEGER))).getMethodHandle();
        Assert.assertEquals(2, MapValueCountFunction.valueCount(INTEGER, equalsMethod, mapBlock, 13L));
        Assert.assertEquals(1, MapValueCountFunction.valueCount(INTEGER, equalsMethod, mapBlock, (Long) null));
    }

    @Test
    public void testValueCount3() throws Exception {
        Map<Integer, Boolean> value = Maps.newHashMap();
        value.put(1, true);
        value.put(2, true);
        value.put(3, false);
        value.put(4, null);
        Block mapBlock = mapBlockOf(INTEGER, BOOLEAN, value);

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        FunctionListBuilder builder = new FunctionListBuilder();
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(BOOLEAN, BOOLEAN))).getMethodHandle();
        Assert.assertEquals(1, MapValueCountFunction.valueCount(BOOLEAN, equalsMethod, mapBlock, false));
//        Assert.assertEquals(1, MapValueCountFunction.valueCount(BOOLEAN, equalsMethod, mapBlock, (Boolean) null));
    }

    @Test
    public void testValueCount4() throws Exception {
        Map<Integer, Double> value = Maps.newHashMap();
        value.put(1, 1.0);
        value.put(2, 2.0);
        value.put(3, null);
        Block mapBlock = mapBlockOf(INTEGER, DOUBLE, value);

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        FunctionListBuilder builder = new FunctionListBuilder();
        functionRegistry.addFunctions(builder.getFunctions());
        MethodHandle equalsMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(DOUBLE, DOUBLE))).getMethodHandle();
        Assert.assertEquals(1, MapValueCountFunction.valueCount(DOUBLE, equalsMethod, mapBlock, 2.0));
        Assert.assertEquals(1, MapValueCountFunction.valueCount(DOUBLE, equalsMethod, mapBlock, (Double) null));
    }
}