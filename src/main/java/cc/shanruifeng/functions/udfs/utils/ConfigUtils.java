package cc.shanruifeng.functions.udfs.utils;

import cc.shanruifeng.functions.udfs.model.ChinaIdArea;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author ruifeng.shan
 * @date 2016-07-07
 * @time 16:21
 */
public class ConfigUtils {
    private static Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    public static List<String> loadFile(String fileName) throws IOException {
        ArrayList<String> strings = Lists.newArrayList();

        Closer closer = Closer.create();
        try {
            InputStream inputStream = ConfigUtils.class.getResourceAsStream(fileName);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            closer.register(bufferedReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                strings.add(line);
            }
        } catch (Throwable t) {
            logger.error("loadFile {} error. error is {}.", fileName, t);
            throw closer.rethrow(t);
        } finally {
            closer.close();
        }

        return strings;
    }

    public static Map<String, ChinaIdArea> getIdCardMap() {
        String fileName = "/china_p_c_a.config";
        Map<String, ChinaIdArea> map = Maps.newHashMap();
        try {
            List<String> list = loadFile(fileName);
            for (String line : list) {
                String[] results = line.split("\t", 4);
                map.put(results[0], new ChinaIdArea(results[1], results[2], results[3]));
            }
        } catch (IOException e) {
            logger.error("get china id card map error. error is {}.", e);
            return map;
        }

        return map;
    }
}
