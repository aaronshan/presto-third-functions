package cc.shanruifeng.functions.udfs;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * @author ruifeng.shan
 * @date 2016-07-06
 * @time 18:39
 */
public class UdfPlugin implements Plugin {
    TypeManager typeManager;

    @Inject
    public void setTypeManager(TypeManager typeManager) {
        this.typeManager = Preconditions.checkNotNull(typeManager, "typeManager is null");
    }

    public void setOptionalConfig(Map<String, String> optionalConfig) {
    }

    public <T> List<T> getServices(Class<T> type) {
        if (type == FunctionFactory.class) {
            return ImmutableList.of(type.cast(new UdfFactory(typeManager)));
        }

        return ImmutableList.of();
    }
}