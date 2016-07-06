package cc.shanruifeng.functions.udfs;

import com.facebook.presto.spi.type.Type;
import java.util.List;

/**
 * @author ruifeng.shan
 * @date 2016-07-06
 * @time 18:55
 */
public interface WindowFunctionDefinition {
    public String getName();

    public Type getReturnType();

    public List<? extends Type> getArgumentTypes();
}
