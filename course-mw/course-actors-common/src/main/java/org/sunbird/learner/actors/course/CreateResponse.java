package org.sunbird.learner.actors.course;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.keys.SunbirdKey;

import java.text.MessageFormat;

public class CreateResponse {

    public static Response createResponse(HttpResponse<String> Response, ObjectMapper mapper) throws Exception {
        try {
            Response response = mapper.readValue(Response.getBody(), Response.class);

            ProjectLogger.log(
                    "Sized: CreateResponse:createResponse : size of response : "
                            + Response.getBody().getBytes().length,
                    LoggerEnum.INFO);

            if (response.getResponseCode().getResponseCode() == ResponseCode.OK.getResponseCode()) {
                response.getResult().remove(SunbirdKey.NODE_ID);
                response.getResult().put(SunbirdKey.COURSE_ID, response.get(SunbirdKey.IDENTIFIER));
                response.getResult().put(SunbirdKey.IDENTIFIER, response.get(SunbirdKey.IDENTIFIER));
                response.getResult().put(SunbirdKey.VERSION_KEY, response.get(SunbirdKey.VERSION_KEY));
            } else {
                String message = ResponseMessages.getErrorMessages(response, "Course creation failed ");
                ProjectCommonException.throwClientErrorException(
                        ResponseCode.customServerError,
                        MessageFormat.format(
                                ResponseCode.customServerError.getErrorMessage(), message));
            }
            return response;
        } catch (Exception ex) {
            throw ex;
        }
    }
}
