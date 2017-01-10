package cc.shanruifeng.functions.udfs.scalar.json;

import cc.shanruifeng.functions.udfs.presto.json.JsonExtract;
import cc.shanruifeng.functions.udfs.presto.json.JsonPath;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import io.airlift.slice.Slice;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static cc.shanruifeng.functions.udfs.utils.JsonUtil.createJsonParser;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static io.airlift.slice.Slices.utf8Slice;

/**
 * @author ruifeng.shan
 * @date 2016-07-21
 * @time 15:43
 */
public class JsonArrayExtractFunction {
    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);
    private static final JsonFactory MAPPING_JSON_FACTORY = new MappingJsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    private static Long jsonArrayLength(@SqlType(StandardTypes.JSON) Slice json) {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }
            long length = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return length;
                }
                parser.skipChildren();

                length++;
            }
        } catch (IOException e) {
            return null;
        }
    }

    private static Slice varcharJsonArrayGet(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.BIGINT) long index) {
        return jsonArrayGet(json, index);
    }

    private static Slice jsonArrayGet(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.BIGINT) long index) {
        // this value cannot be converted to positive number
        if (index == Long.MIN_VALUE) {
            return null;
        }

        try (JsonParser parser = createJsonParser(MAPPING_JSON_FACTORY, json)) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            List<String> tokens = null;
            if (index < 0) {
                tokens = new LinkedList<>();
            }

            long count = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    if (tokens != null && count >= index * -1) {
                        return utf8Slice(tokens.get(0));
                    }

                    return null;
                }

                String arrayElement;
                if (token == START_OBJECT || token == START_ARRAY) {
                    arrayElement = parser.readValueAsTree().toString();
                } else {
                    arrayElement = parser.getValueAsString();
                }

                if (count == index) {
                    return arrayElement == null ? null : utf8Slice(arrayElement);
                }

                if (tokens != null) {
                    tokens.add(arrayElement);

                    if (count >= index * -1) {
                        tokens.remove(0);
                    }
                }

                count++;
            }
        } catch (IOException e) {
            return null;
        }
    }

    private static Slice varcharJsonExtract(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPathSlice) {
        JsonPath jsonPath = new JsonPath(jsonPathSlice.toStringUtf8());
        return JsonExtract.extract(json, jsonPath.getObjectExtractor());
    }

    private static Slice varcharJsonExtractScalar(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPathSlice) {
        JsonPath jsonPath = new JsonPath(jsonPathSlice.toStringUtf8());
        return JsonExtract.extract(json, jsonPath.getScalarExtractor());
    }

    @ScalarFunction("json_array_extract")
    @Description("extract json array value by given jsonPath.")
    @SqlType("array(varchar)")
    public static Block jsonArrayExtract(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPathSlice) {
        Long length = jsonArrayLength(json);
        if (length == null) {
            return null;
        }
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), length.intValue());
        for (int i = 0; i < length; i++) {
            Slice content = varcharJsonArrayGet(json, i);
            Slice result = varcharJsonExtract(content, jsonPathSlice);
            if (result == null) {
                blockBuilder.appendNull();
            } else {
                VARCHAR.writeSlice(blockBuilder, result);
            }
        }
        return blockBuilder.build();
    }

    @ScalarFunction("json_array_extract_scalar")
    @Description("extract json array value by given jsonPath.")
    @SqlType("array(varchar)")
    public static Block jsonArrayExtractScalar(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPathSlice) {
        Long length = jsonArrayLength(json);
        if (length == null) {
            return null;
        }
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), length.intValue());
        for (int i = 0; i < length; i++) {
            Slice content = varcharJsonArrayGet(json, i);
            Slice result = varcharJsonExtractScalar(content, jsonPathSlice);
            if (result == null) {
                blockBuilder.appendNull();
            } else {
                VARCHAR.writeSlice(blockBuilder, result);
            }
        }
        return blockBuilder.build();
    }
}