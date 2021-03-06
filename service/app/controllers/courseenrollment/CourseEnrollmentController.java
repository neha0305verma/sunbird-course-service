package controllers.courseenrollment;

import controllers.BaseController;
import controllers.courseenrollment.validator.CourseEnrollmentRequestValidator;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.Request;
import java.util.concurrent.CompletionStage;

import play.mvc.Http;
import play.mvc.Result;

public class CourseEnrollmentController extends BaseController {

  public CompletionStage<Result> getEnrolledCourses(String uid, Http.Request httpRequest) {
    return handleRequest(
        ActorOperations.GET_COURSE.getValue(),
        httpRequest.body().asJson(),
        (req) -> {
          Request request = (Request) req;
          request
              .getContext()
              .put(JsonKey.URL_QUERY_STRING, getQueryString(httpRequest.queryString()));
          request
              .getContext()
              .put(JsonKey.BATCH_DETAILS, httpRequest.queryString().get(JsonKey.BATCH_DETAILS));
          return null;
        },
        ProjectUtil.getLmsUserId(uid),
        JsonKey.USER_ID,
        getAllRequestHeaders((httpRequest)),
        false,
            httpRequest);
  }

  public CompletionStage<Result> getEnrolledCourse(Http.Request httpRequest) {
    return handleRequest(
        ActorOperations.GET_USER_COURSE.getValue(),
        httpRequest.body().asJson(),
        (req) -> {
          Request request = (Request) req;
          new CourseEnrollmentRequestValidator().validateEnrolledCourse(request);
          return null;
        },
        getAllRequestHeaders((httpRequest)),
            httpRequest);
  }

  public CompletionStage<Result> enrollCourse(Http.Request httpRequest) {
    return handleRequest(
        ActorOperations.ENROLL_COURSE.getValue(),
        httpRequest.body().asJson(),
        (request) -> {
          new CourseEnrollmentRequestValidator().validateEnrollCourse((Request) request);
          return null;
        },
        getAllRequestHeaders(httpRequest),
            httpRequest);
  }

  public CompletionStage<Result> unenrollCourse(Http.Request httpRequest) {
    return handleRequest(
        ActorOperations.UNENROLL_COURSE.getValue(),
        httpRequest.body().asJson(),
        (request) -> {
          new CourseEnrollmentRequestValidator().validateUnenrollCourse((Request) request);
          return null;
        },
        getAllRequestHeaders(httpRequest),
            httpRequest);
  }
}
