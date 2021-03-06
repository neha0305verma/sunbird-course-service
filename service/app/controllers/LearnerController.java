/** */
package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.LearnerStateRequestValidator;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.RequestValidator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

/**
 * This controller will handler all the request related to learner state.
 *
 * @author Manzarul
 */
public class LearnerController extends BaseController {

    private LearnerStateRequestValidator validator = new LearnerStateRequestValidator();

    /**
     * This method will provide list of user content state. Content refer user activity {started,half
     * completed ,completed} against TOC (table of content).
     *
     * @return Result
     */
    public CompletionStage<Result> getContentState(Http.Request httpRequest) {
        try {
            JsonNode requestJson = httpRequest.body().asJson();
            Request request = createAndInitRequest(ActorOperations.GET_CONTENT.getValue(), requestJson, httpRequest);
            validator.validateGetContentState(request);
            request = transformUserId(request);
            return actorResponseHandler(getActorRef(), request, timeout, JsonKey.CONTENT_LIST, httpRequest);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest));
        }
    }

    /**
     * This method will update learner current state with last store state.
     *
     * @return Result
     */
    public CompletionStage<Result> updateContentState(Http.Request httpRequest) {
        try {
            JsonNode requestData = httpRequest.body().asJson();
            ProjectLogger.log(" updateContentState request data=" + requestData, LoggerEnum.INFO.name());
            Request reqObj = (Request) mapper.RequestMapper.mapRequest(requestData, Request.class);
            RequestValidator.validateUpdateContent(reqObj);
            reqObj = transformUserId(reqObj);
            reqObj.setOperation(ActorOperations.ADD_CONTENT.getValue());
            reqObj.setRequestId(ExecutionContext.getRequestId());
            reqObj.setEnv(getEnvironment());
            HashMap<String, Object> innerMap = new HashMap<>();
            innerMap.put(JsonKey.CONTENTS, reqObj.getRequest().get(JsonKey.CONTENTS));
            innerMap.put(JsonKey.REQUESTED_BY, httpRequest.flash().get(JsonKey.USER_ID));
            innerMap.put(JsonKey.ASSESSMENT_EVENTS, reqObj.getRequest().get(JsonKey.ASSESSMENT_EVENTS));
            innerMap.put(JsonKey.USER_ID, reqObj.getRequest().get(JsonKey.USER_ID));
            reqObj.setRequest(innerMap);
            return actorResponseHandler(getActorRef(), reqObj, timeout, null, httpRequest);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest));
        }
    }

    public Result getHealth() {
        return Results.ok("ok");
    }

    /**
     * @param all
     * @return
     */
    public Result preflight(String all) {
        return ok().withHeader("Access-Control-Allow-Origin", "*")
                .withHeader("Allow", "*")
                .withHeader("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS")
                .withHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Referer, User-Agent,X-Consumer-ID,cid,ts,X-Device-ID,X-Authenticated-Userid,X-msgid,id,X-Access-TokenId");
    }
}
