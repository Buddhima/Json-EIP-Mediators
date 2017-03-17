package com.buddhima;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;

import java.util.ArrayList;
import java.util.List;

public class JsonIterateUtils {

    public static List getDetachedMatchingElements(String jsonPayload, String expression, Configuration configuration) {

        List<Object> elementList = new ArrayList<Object>();

        Object o = JsonPath.using(configuration).parse(jsonPayload).read(expression);

        if (o instanceof List) {
            for (Object elem : (List) o) {
                elementList.add(elem);
            }
        } else {
            elementList.add(o);
        }

        return elementList;
    }
}
