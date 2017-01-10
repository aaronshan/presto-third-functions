package cc.shanruifeng.functions.udfs.scalar.date;

import com.facebook.presto.metadata.FunctionListBuilder;
import io.airlift.slice.Slices;
import junit.framework.Assert;
import org.junit.Test;

/**
 * @author ruifeng.shan
 * @date 2016-07-15
 * @time 14:48
 */
public class ChinaTypeOfDayFunctionTest {
    @Test
    public void testFunctionCreate() throws Exception {
        FunctionListBuilder builder = new FunctionListBuilder();
        builder.scalars(ChinaTypeOfDayFunction.class);
    }

    @Test
    public void testTypeOfDay() {
        Assert.assertEquals(1, ChinaTypeOfDayFunction.typeOfDay(Slices.utf8Slice("2016-10-01")));
        Assert.assertEquals(2, ChinaTypeOfDayFunction.typeOfDay(Slices.utf8Slice("2016-07-16")));
        Assert.assertEquals(3, ChinaTypeOfDayFunction.typeOfDay(Slices.utf8Slice("2016-07-15")));
        Assert.assertEquals(4, ChinaTypeOfDayFunction.typeOfDay(Slices.utf8Slice("2016-09-18")));
    }
}