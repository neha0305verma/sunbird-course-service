package org.sunbird.learner.actors.course;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.base.BaseActor;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;;
import org.sunbird.keys.*;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.util.Util;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.sunbird.common.models.util.JsonKey.EKSTEP_BASE_URL;
import static org.sunbird.common.models.util.ProjectUtil.getConfigValue;
import static org.sunbird.common.responsecode.ResponseCode.SERVER_ERROR;

public class CourseManagementActor extends BaseActor {
    private static ObjectMapper mapper = new ObjectMapper();
    private String copyUrl = "/content/v3/copy/";
    private String createUrl = "/content/v3/create";

    @Override
    public void onReceive(Request request) throws Throwable {
        Util.initializeContext(request, TelemetryEnvKey.COURSE_CREATE);
        ExecutionContext.setRequestId(request.getRequestId());
        String requestedOperation = request.getOperation();
        switch (requestedOperation) {
            case "createCourse":
                createCourse(request);
                break;
            default:
                onReceiveUnsupportedOperation(requestedOperation);
                break;
        }
    }

    private void createCourse(Request request) throws Exception {
        Map<String, Object> contentMap = new HashMap<>();
        contentMap.putAll((Map<String, Object>) request.get(SunbirdKey.COURSE));
        Map<String, Object> requestMap = new HashMap<String, Object>() {{
            put(SunbirdKey.REQUEST, new HashMap<String, Object>() {{
                put(SunbirdKey.CONTENT, contentMap);
            }});
        }};
        try {
            HttpResponse<String> updateResponse =
                    Unirest.post(getRequestUrl(request, contentMap))
                            .headers(getHeaders((String) request.getContext().get(SunbirdKey.CHANNEL)))
                            .body(mapper.writeValueAsString(requestMap))
                            .asString();

            ProjectLogger.log(
                    "CourseManagementActor:createCourse : Request for course create : "
                            + mapper.writeValueAsString(requestMap),
                    LoggerEnum.INFO.name());
            ProjectLogger.log(
                    "Sized: CourseManagementActor:createCourse : size of request : "
                            + mapper.writeValueAsString(requestMap).getBytes().length,
                    LoggerEnum.INFO);

            Response response = validateUpdateResponse(updateResponse);
            if (request.getRequest().containsKey(SunbirdKey.SOURCE)) {
                Map<String, Object> node_id = (Map<String, Object>) response.get(SunbirdKey.NODE_ID);
                response.put(SunbirdKey.IDENTIFIER, node_id.get(request.get(SunbirdKey.SOURCE)));
            }
            sender().tell(response, self());
        } catch (Exception ex) {
            ProjectLogger.log("CourseManagementActor:createCourse : course create error ", ex);
            if (ex instanceof ProjectCommonException) {
                throw ex;
            } else {
                throw new ProjectCommonException(
                        ResponseCode.CLIENT_ERROR.getErrorCode(),
                        ResponseCode.CLIENT_ERROR.getErrorMessage(),
                        SERVER_ERROR.getResponseCode());
            }
        }
    }

    private String getRequestUrl(Request request, Map<String, Object> contentMap) {
        String requestUrl = getConfigValue(EKSTEP_BASE_URL);
        if (request.getRequest().containsKey(SunbirdKey.SOURCE)) {
            if (!((Map<String, Object>) request.get(SunbirdKey.COURSE)).containsKey(SunbirdKey.COPY_SCHEME)) {
                contentMap.put(SunbirdKey.COPY_SCHEME, SunbirdKey.TEXT_BOOK_TO_COURSE);
            }
            return requestUrl + copyUrl + request.get(SunbirdKey.SOURCE) + "?type=deep";
        }
        return requestUrl + createUrl;
    }

    private Response validateUpdateResponse(HttpResponse<String> updateResponse) throws Exception{
        if (null != updateResponse) {
            return CreateResponse.createResponse(updateResponse, mapper);
        } else {
            throw new ProjectCommonException(
                    ResponseCode.CLIENT_ERROR.getErrorCode(),
                    ResponseCode.CLIENT_ERROR.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }

    private Map<String, String> getHeaders(String channel) {
        Map<String, String> headers = new HashMap<String, String>() {{
            put(SunbirdKey.CONTENT_TYPE_HEADER, SunbirdKey.APPLICATION_JSON);
            put(SunbirdKey.X_CHANNEL_ID, channel);
        }};
        return headers;
    }

}