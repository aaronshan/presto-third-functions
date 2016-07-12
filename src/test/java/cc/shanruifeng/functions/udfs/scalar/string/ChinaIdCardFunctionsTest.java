package cc.shanruifeng.functions.udfs.scalar.string;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * @author ruifeng.shan
 * @date 2016-07-12
 * @time 14:14
 */
public class ChinaIdCardFunctionsTest {

    @Test
    public void testFunctionCreate() throws Exception {
        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionListBuilder builder = new FunctionListBuilder(typeRegistry);
        builder.scalar(ChinaIdCardFunctions.class);
    }

    @Test
    public void testGetIdCardProvince() throws Exception {
        Slice result = ChinaIdCardFunctions.getIdCardProvince(Slices.utf8Slice("110101198901084517"));
        Assert.assertEquals("北京市", result.toStringUtf8());
    }

    @Test
    public void testGetIdCardCity() throws Exception {
        Slice result = ChinaIdCardFunctions.getIdCardCity(Slices.utf8Slice("110101198901084517"));
        Assert.assertEquals("北京市", result.toStringUtf8());
    }

    @Test
    public void testGetIdCardArea() throws Exception {
        Slice result = ChinaIdCardFunctions.getIdCardArea(Slices.utf8Slice("110101198901084517"));
        Assert.assertEquals("东城区", result.toStringUtf8());
    }

    @Test
    public void testGetIdCardBirthday() throws Exception {
        Slice result = ChinaIdCardFunctions.getIdCardBirthday(Slices.utf8Slice("110101198901084517"));
        Assert.assertEquals("19890108", result.toStringUtf8());
    }

    @Test
    public void testGetIdCardGender() throws Exception {
        Slice result = ChinaIdCardFunctions.getIdCardGender(Slices.utf8Slice("110101198901084517"));
        Assert.assertEquals("男", result.toStringUtf8());
    }

    @Test
    public void testIsValidIdCard() throws Exception {
        boolean result = ChinaIdCardFunctions.isValidIdCard(Slices.utf8Slice("110101198901084517"));
        Assert.assertEquals(true, result);
    }

    @Test
    public void testGetIdInfo() throws Exception {
        Slice result = ChinaIdCardFunctions.getJsonOfChinaIdCard(Slices.utf8Slice("110101198901084517"));
        ObjectMapper mapper = new ObjectMapper();
        Assert.assertEquals(true,mapper.readValue(result.toStringUtf8(), Map.class).get("valid"));
    }
}