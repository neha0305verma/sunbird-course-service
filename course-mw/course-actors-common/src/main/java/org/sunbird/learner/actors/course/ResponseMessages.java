package org.sunbird.learner.actors.course;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.response.Response;
import org.sunbird.keys.SunbirdKey;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ResponseMessages {

    public static String getErrorMessages(Response response, String message) {
        Map<String, Object> resultMap =
                Optional.ofNullable(response.getResult()).orElse(new HashMap<>());
        if (MapUtils.isNotEmpty(resultMap)) {
            Object obj = Optional.ofNullable(resultMap.get(SunbirdKey.TB_MESSAGES)).orElse("");
            String msg = obj instanceof List ? ((List<String>) obj).stream().collect(Collectors.joining(";"))
                    : response.getParams().getErrmsg();
            message += StringUtils.isNotEmpty(msg) ? msg : String.valueOf(obj);
        }
        return message;
    }
}
