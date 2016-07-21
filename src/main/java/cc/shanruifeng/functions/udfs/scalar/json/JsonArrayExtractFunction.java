package cc.shanruifeng.functions.udfs.scalar.json;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.JsonFunctions;
import com.facebook.presto.operator.scalar.JsonPath;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.JsonPathType;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;

/**
 * @author ruifeng.shan
 * @date 2016-07-21
 * @time 15:43
 */
public class JsonArrayExtractFunction {
    @ScalarFunction("json_array_extract")
    @Description("extract json array value by given jsonPath.")
    @SqlType("array(varchar)")
    public static Block jsonArrayExtract(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath) {
        Long length = JsonFunctions.jsonArrayLength(json);
        if (length == null) {
            return null;
        }
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), length.intValue());
        for (int i = 0; i < length; i++) {
            Slice content = JsonFunctions.varcharJsonArrayGet(json, i);
            Slice result = JsonFunctions.varcharJsonExtract(content, jsonPath);
            appendToBlockBuilder(VARCHAR, result, blockBuilder);
        }
        return blockBuilder.build();
    }

    @ScalarFunction("json_array_extract_scalar")
    @Description("extract json array value by given jsonPath.")
    @SqlType("array(varchar)")
    public static Block jsonArrayExtractScalar(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath) {
        Long length = JsonFunctions.jsonArrayLength(json);
        if (length == null) {
            return null;
        }
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), length.intValue());
        for (int i = 0; i < length; i++) {
            Slice content = JsonFunctions.varcharJsonArrayGet(json, i);
            Slice result = JsonFunctions.varcharJsonExtractScalar(content, jsonPath);
            appendToBlockBuilder(VARCHAR, result, blockBuilder);
        }
        return blockBuilder.build();
    }
}