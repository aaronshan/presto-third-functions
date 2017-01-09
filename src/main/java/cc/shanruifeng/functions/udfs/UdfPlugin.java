package cc.shanruifeng.functions.udfs;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ruifeng.shan
 * @date 2016-07-06
 * @time 18:39
 */
public class UdfPlugin implements Plugin {
    private static Logger logger = LoggerFactory.getLogger(UdfPlugin.class);

    private List<Class<?>> getFunctionClasses() throws IOException {
        List<Class<?>> classes = Lists.newArrayList();
        String classResource = this.getClass().getName().replace(".", "/") + ".class";
        String jarURLFile = Thread.currentThread().getContextClassLoader().getResource(classResource).getFile();
        int jarEnd = jarURLFile.indexOf('!');
        String jarLocation = jarURLFile.substring(0, jarEnd); // This is in URL format, convert once more to get actual file location
        jarLocation = new URL(jarLocation).getFile();

        ZipInputStream zip = new ZipInputStream(new FileInputStream(jarLocation));
        for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
            if (entry.getName().endsWith(".class") && !entry.isDirectory()) {
                String className = entry.getName().replace("/", "."); // This still has .class at the end
                className = className.substring(0, className.length() - 6); // remvove .class from end
                try {
                    classes.add(Class.forName(className));
                } catch (ClassNotFoundException e) {
                    logger.error("Could not load class {}, Exception: {}", className, e);
                }
            }
        }
        return classes;
    }

    @Override
    public Set<Class<?>> getFunctions() {
        try {
            List<Class<?>> classes = getFunctionClasses();
            Set<Class<?>> set = Sets.newHashSet();
            for (Class<?> clazz : classes) {
                logger.info("Adding: " + clazz);
                if (clazz.getName().startsWith("cc.shanruifeng.functions.udfs.scalar")
                        || clazz.getName().startsWith("cc.shanruifeng.functions.udfs.aggregation")
                        || clazz.getName().startsWith("cc.shanruifeng.functions.udfs.window")) {
                    set.add(clazz);
                }
            }
            return ImmutableSet.<Class<?>>builder().addAll(set).build();
        } catch (IOException e) {
            logger.error("Could not load classes from jar file: {} ", e);
            return ImmutableSet.of();
        }

    }
}